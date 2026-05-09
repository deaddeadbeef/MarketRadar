# Phase 1 Deterministic MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Catalyst Radar Phase 1: a deterministic-only scanner that ingests securities and daily bars from local CSV, computes market features, applies scoring, policy, and portfolio gates, renders a review dashboard, and validates point-in-time behavior with tests.

**Architecture:** Implement a Python package under `src/catalyst_radar` with focused modules for configuration, domain models, storage, connectors, features, scoring, portfolio risk, scanning, validation, and dashboard data access. Phase 1 uses local CSV inputs as the first connector because it is deterministic, credential-free, and testable; the connector boundary is designed so paid/live market data providers can be added without changing feature or policy code. No LLM calls are included in Phase 1.

**Tech Stack:** Python 3.11, pandas, numpy, SQLAlchemy Core, psycopg, Streamlit, pytest, ruff, Docker Compose with Postgres for local integration.

---

## Scope Boundary

Phase 1 builds the deterministic foundation only.

In scope:

- Python project scaffold.
- Local configuration and secrets conventions.
- SQLAlchemy schema and matching Postgres migration.
- CSV securities and daily-bar ingestion.
- Market feature computation for price strength, relative strength, volume, volatility, liquidity, and setup flags.
- Deterministic scoring.
- Policy states: `NoAction`, `ResearchOnly`, `AddToWatchlist`, `Warning`, `EligibleForManualBuyReview`, `Blocked`.
- Portfolio risk gates using manually supplied portfolio settings and optional holdings CSV.
- Scanner pipeline that writes signal features and candidate states.
- Streamlit review dashboard.
- Point-in-time validation skeleton and baseline definitions.
- Unit, integration, and golden tests.

Out of scope:

- SEC/news connectors.
- Local NLP and embeddings.
- LLM router, EvidencePacket generation, and Decision Cards.
- Broker integration.
- Options trade recommendations.
- Real-money workflow.

## Target File Map

Create this structure:

```text
MarketRadar/
  .env.example
  .gitignore
  README.md
  docker-compose.yml
  pyproject.toml
  sql/
    migrations/
      001_initial.sql
  data/
    sample/
      securities.csv
      daily_bars.csv
      holdings.csv
  src/
    catalyst_radar/
      __init__.py
      cli.py
      core/
        __init__.py
        config.py
        models.py
      storage/
        __init__.py
        db.py
        repositories.py
        schema.py
      connectors/
        __init__.py
        csv_market.py
      features/
        __init__.py
        market.py
      scoring/
        __init__.py
        policy.py
        score.py
      portfolio/
        __init__.py
        risk.py
      pipeline/
        __init__.py
        scan.py
      validation/
        __init__.py
        backtest.py
      dashboard/
        __init__.py
        data.py
  apps/
    dashboard/
      Home.py
  tests/
    fixtures/
      securities.csv
      daily_bars.csv
      holdings.csv
    unit/
      test_config.py
      test_models.py
      test_market_features.py
      test_policy.py
      test_score.py
      test_portfolio.py
      test_backtest.py
    integration/
      test_csv_ingest.py
      test_scan_pipeline.py
```

## Task 1: Project Foundation

**Files:**

- Create: `.gitignore`
- Create: `.env.example`
- Create: `pyproject.toml`
- Create: `README.md`
- Create: `docker-compose.yml`
- Create: `src/catalyst_radar/__init__.py`
- Create: package `__init__.py` files under every package directory
- Create: `data/sample/securities.csv`
- Create: `data/sample/daily_bars.csv`
- Create: `data/sample/holdings.csv`
- Create: `tests/fixtures/securities.csv`
- Create: `tests/fixtures/daily_bars.csv`
- Create: `tests/fixtures/holdings.csv`

- [ ] **Step 1: Initialize git**

Run:

```powershell
git init
git status --short
```

Expected: `git status --short` prints no tracked files yet, or only files already created by the user.

- [ ] **Step 2: Create `.gitignore`**

Write:

```gitignore
.env
.env.local
.venv/
__pycache__/
*.py[cod]
.pytest_cache/
.ruff_cache/
.mypy_cache/
htmlcov/
.coverage
dist/
build/
*.egg-info/
.streamlit/secrets.toml
data/local/
data/cache/
```

- [ ] **Step 3: Create `.env.example`**

Write:

```dotenv
CATALYST_ENV=local
CATALYST_DATABASE_URL=sqlite:///data/local/catalyst_radar.db
CATALYST_LOG_LEVEL=INFO
CATALYST_PRICE_MIN=5
CATALYST_MARKET_CAP_MIN=300000000
CATALYST_AVG_DOLLAR_VOLUME_MIN=10000000
CATALYST_RISK_PER_TRADE_PCT=0.005
CATALYST_MAX_SINGLE_NAME_PCT=0.08
CATALYST_MAX_SECTOR_PCT=0.30
CATALYST_MAX_THEME_PCT=0.35
CATALYST_ENABLE_PREMIUM_LLM=false
```

- [ ] **Step 4: Create `pyproject.toml`**

Write:

```toml
[build-system]
requires = ["setuptools>=69", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "catalyst-radar"
version = "0.1.0"
description = "Deterministic-first market radar for public-equity opportunity review"
requires-python = ">=3.11"
dependencies = [
  "numpy>=1.26",
  "pandas>=2.2",
  "psycopg[binary]>=3.2",
  "python-dotenv>=1.0",
  "sqlalchemy>=2.0",
  "streamlit>=1.35",
]

[project.optional-dependencies]
dev = [
  "pytest>=8.2",
  "pytest-cov>=5.0",
  "ruff>=0.5",
]

[project.scripts]
catalyst-radar = "catalyst_radar.cli:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]
addopts = "-q"

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]
```

- [ ] **Step 5: Create `docker-compose.yml`**

Write:

```yaml
services:
  postgres:
    image: postgres:16
    container_name: catalyst-radar-postgres
    environment:
      POSTGRES_DB: catalyst_radar
      POSTGRES_USER: catalyst
      POSTGRES_PASSWORD: catalyst
    ports:
      - "54321:5432"
    volumes:
      - catalyst_postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U catalyst -d catalyst_radar"]
      interval: 5s
      timeout: 5s
      retries: 12

volumes:
  catalyst_postgres_data:
```

- [ ] **Step 6: Create `README.md`**

Write:

````markdown
# Catalyst Radar

Catalyst Radar is a deterministic-first market radar for public-equity opportunity review.

Phase 1 builds the scanner, feature engine, policy gates, portfolio risk checks, validation skeleton, and dashboard without LLM calls.

## Local setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
Copy-Item .env.example .env.local
pytest
```

## Local database

SQLite is the default local database:

```powershell
catalyst-radar init-db
catalyst-radar ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
catalyst-radar scan --as-of 2026-05-08
```

Postgres integration is available through Docker Compose:

```powershell
docker compose up -d postgres
$env:CATALYST_DATABASE_URL="postgresql+psycopg://catalyst:catalyst@localhost:54321/catalyst_radar"
catalyst-radar init-db
```

## Dashboard

```powershell
streamlit run apps/dashboard/Home.py
```

## Phase 1 rule

No premium LLM calls are used or required in Phase 1.
````

- [ ] **Step 7: Create package directories and `__init__.py` files**

Create empty `__init__.py` files at:

```text
src/catalyst_radar/__init__.py
src/catalyst_radar/core/__init__.py
src/catalyst_radar/storage/__init__.py
src/catalyst_radar/connectors/__init__.py
src/catalyst_radar/features/__init__.py
src/catalyst_radar/scoring/__init__.py
src/catalyst_radar/portfolio/__init__.py
src/catalyst_radar/pipeline/__init__.py
src/catalyst_radar/validation/__init__.py
src/catalyst_radar/dashboard/__init__.py
```

- [ ] **Step 8: Create sample CSV files**

Write `data/sample/securities.csv` and copy the same content to `tests/fixtures/securities.csv`:

```csv
ticker,name,exchange,sector,industry,market_cap,avg_dollar_volume_20d,has_options,is_active,updated_at
AAA,Alpha Analytics,NASDAQ,Technology,Software,5000000000,50000000,true,true,2026-05-08T20:00:00Z
BBB,Beta Builders,NYSE,Industrials,Construction,2200000000,18000000,true,true,2026-05-08T20:00:00Z
CCC,Coda Components,NASDAQ,Technology,Semiconductors,900000000,7000000,false,true,2026-05-08T20:00:00Z
SPY,SPDR S&P 500 ETF,NYSE Arca,ETF,Index ETF,0,9000000000,true,true,2026-05-08T20:00:00Z
XLK,Technology Select Sector SPDR,NYSE Arca,ETF,Sector ETF,0,1000000000,true,true,2026-05-08T20:00:00Z
XLI,Industrial Select Sector SPDR,NYSE Arca,ETF,Sector ETF,0,800000000,true,true,2026-05-08T20:00:00Z
```

Write `data/sample/holdings.csv` and copy the same content to `tests/fixtures/holdings.csv`:

```csv
ticker,shares,market_value,sector,theme,as_of
AAA,20,2000,Technology,ai_infrastructure,2026-05-08T20:00:00Z
```

Write `data/sample/daily_bars.csv` and copy the same content to `tests/fixtures/daily_bars.csv`:

```csv
ticker,date,open,high,low,close,volume,vwap,adjusted,provider,source_ts,available_at
AAA,2026-05-01,91,95,90,94,500000,93,true,sample,2026-05-01T20:00:00Z,2026-05-01T21:00:00Z
AAA,2026-05-04,94,99,93,98,700000,97,true,sample,2026-05-04T20:00:00Z,2026-05-04T21:00:00Z
AAA,2026-05-05,98,101,97,100,800000,99,true,sample,2026-05-05T20:00:00Z,2026-05-05T21:00:00Z
AAA,2026-05-06,100,103,99,102,850000,101,true,sample,2026-05-06T20:00:00Z,2026-05-06T21:00:00Z
AAA,2026-05-07,102,106,101,105,1100000,104,true,sample,2026-05-07T20:00:00Z,2026-05-07T21:00:00Z
AAA,2026-05-08,105,110,104,109,1500000,108,true,sample,2026-05-08T20:00:00Z,2026-05-08T21:00:00Z
BBB,2026-05-01,40,41,39,40,350000,40,true,sample,2026-05-01T20:00:00Z,2026-05-01T21:00:00Z
BBB,2026-05-04,40,41,39,40,340000,40,true,sample,2026-05-04T20:00:00Z,2026-05-04T21:00:00Z
BBB,2026-05-05,40,42,40,41,450000,41,true,sample,2026-05-05T20:00:00Z,2026-05-05T21:00:00Z
BBB,2026-05-06,41,42,40,41,420000,41,true,sample,2026-05-06T20:00:00Z,2026-05-06T21:00:00Z
BBB,2026-05-07,41,42,40,41,410000,41,true,sample,2026-05-07T20:00:00Z,2026-05-07T21:00:00Z
BBB,2026-05-08,41,42,40,41,430000,41,true,sample,2026-05-08T20:00:00Z,2026-05-08T21:00:00Z
CCC,2026-05-01,8,8.5,7.8,8.1,250000,8.1,true,sample,2026-05-01T20:00:00Z,2026-05-01T21:00:00Z
CCC,2026-05-04,8.1,8.3,7.9,8.0,180000,8.0,true,sample,2026-05-04T20:00:00Z,2026-05-04T21:00:00Z
CCC,2026-05-05,8.0,8.2,7.8,8.0,160000,8.0,true,sample,2026-05-05T20:00:00Z,2026-05-05T21:00:00Z
CCC,2026-05-06,8.0,8.1,7.7,7.9,150000,7.9,true,sample,2026-05-06T20:00:00Z,2026-05-06T21:00:00Z
CCC,2026-05-07,7.9,8.0,7.6,7.8,140000,7.8,true,sample,2026-05-07T20:00:00Z,2026-05-07T21:00:00Z
CCC,2026-05-08,7.8,7.9,7.5,7.6,130000,7.6,true,sample,2026-05-08T20:00:00Z,2026-05-08T21:00:00Z
SPY,2026-05-01,500,505,499,504,70000000,503,true,sample,2026-05-01T20:00:00Z,2026-05-01T21:00:00Z
SPY,2026-05-04,504,508,503,507,72000000,506,true,sample,2026-05-04T20:00:00Z,2026-05-04T21:00:00Z
SPY,2026-05-05,507,509,505,506,71000000,507,true,sample,2026-05-05T20:00:00Z,2026-05-05T21:00:00Z
SPY,2026-05-06,506,510,505,509,73000000,508,true,sample,2026-05-06T20:00:00Z,2026-05-06T21:00:00Z
SPY,2026-05-07,509,511,507,510,74000000,509,true,sample,2026-05-07T20:00:00Z,2026-05-07T21:00:00Z
SPY,2026-05-08,510,513,509,512,76000000,512,true,sample,2026-05-08T20:00:00Z,2026-05-08T21:00:00Z
XLK,2026-05-01,200,203,199,202,10000000,202,true,sample,2026-05-01T20:00:00Z,2026-05-01T21:00:00Z
XLK,2026-05-04,202,206,201,205,11000000,204,true,sample,2026-05-04T20:00:00Z,2026-05-04T21:00:00Z
XLK,2026-05-05,205,207,204,206,10500000,206,true,sample,2026-05-05T20:00:00Z,2026-05-05T21:00:00Z
XLK,2026-05-06,206,210,205,209,12000000,208,true,sample,2026-05-06T20:00:00Z,2026-05-06T21:00:00Z
XLK,2026-05-07,209,212,208,211,13000000,211,true,sample,2026-05-07T20:00:00Z,2026-05-07T21:00:00Z
XLK,2026-05-08,211,216,210,215,14000000,214,true,sample,2026-05-08T20:00:00Z,2026-05-08T21:00:00Z
XLI,2026-05-01,120,121,119,120,9000000,120,true,sample,2026-05-01T20:00:00Z,2026-05-01T21:00:00Z
XLI,2026-05-04,120,122,119,121,9200000,121,true,sample,2026-05-04T20:00:00Z,2026-05-04T21:00:00Z
XLI,2026-05-05,121,122,120,121,9100000,121,true,sample,2026-05-05T20:00:00Z,2026-05-05T21:00:00Z
XLI,2026-05-06,121,123,120,122,9300000,122,true,sample,2026-05-06T20:00:00Z,2026-05-06T21:00:00Z
XLI,2026-05-07,122,123,121,122,9400000,122,true,sample,2026-05-07T20:00:00Z,2026-05-07T21:00:00Z
XLI,2026-05-08,122,124,121,123,9500000,123,true,sample,2026-05-08T20:00:00Z,2026-05-08T21:00:00Z
```

- [ ] **Step 9: Install and run baseline checks**

Run:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -m pytest
python -m ruff check .
```

Expected: `pytest` reports no collected tests or passes if tests have already been added. `ruff` passes for the files created so far.

- [ ] **Step 10: Commit foundation**

Run:

```powershell
git add .
git commit -m "chore: scaffold catalyst radar phase 1"
```

Expected: commit succeeds.

## Task 2: Core Config and Domain Models

**Files:**

- Create: `src/catalyst_radar/core/config.py`
- Create: `src/catalyst_radar/core/models.py`
- Test: `tests/unit/test_config.py`
- Test: `tests/unit/test_models.py`

- [ ] **Step 1: Write failing config tests**

Create `tests/unit/test_config.py`:

```python
from catalyst_radar.core.config import AppConfig


def test_config_defaults_are_deterministic_only() -> None:
    config = AppConfig.from_env({})

    assert config.environment == "local"
    assert config.enable_premium_llm is False
    assert config.price_min == 5
    assert config.avg_dollar_volume_min == 10_000_000


def test_config_reads_risk_settings_from_env() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_RISK_PER_TRADE_PCT": "0.01",
            "CATALYST_MAX_SINGLE_NAME_PCT": "0.05",
            "CATALYST_MAX_SECTOR_PCT": "0.25",
        }
    )

    assert config.risk_per_trade_pct == 0.01
    assert config.max_single_name_pct == 0.05
    assert config.max_sector_pct == 0.25
```

- [ ] **Step 2: Write failing model tests**

Create `tests/unit/test_models.py`:

```python
from datetime import UTC, datetime

from catalyst_radar.core.models import (
    ActionState,
    CandidateSnapshot,
    MarketFeatures,
    PolicyResult,
)


def test_action_state_values_are_stable() -> None:
    assert ActionState.NO_ACTION.value == "NoAction"
    assert ActionState.ADD_TO_WATCHLIST.value == "AddToWatchlist"
    assert ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW.value == "EligibleForManualBuyReview"
    assert ActionState.BLOCKED.value == "Blocked"


def test_policy_result_records_block_reasons() -> None:
    result = PolicyResult(
        state=ActionState.BLOCKED,
        hard_blocks=("liquidity_hard_block",),
        reasons=("avg dollar volume below floor",),
    )

    assert result.is_blocked is True
    assert result.hard_blocks == ("liquidity_hard_block",)


def test_candidate_snapshot_keeps_availability_time() -> None:
    as_of = datetime(2026, 5, 8, 21, tzinfo=UTC)
    features = MarketFeatures(
        ticker="AAA",
        as_of=as_of,
        ret_5d=0.12,
        ret_20d=0.12,
        rs_20_sector=82,
        rs_60_spy=80,
        near_52w_high=0.98,
        ma_regime=90,
        rel_volume_5d=2.0,
        dollar_volume_z=2.5,
        atr_pct=0.04,
        extension_20d=0.08,
        liquidity_score=90,
        feature_version="market-v1",
    )
    snapshot = CandidateSnapshot(
        ticker="AAA",
        as_of=as_of,
        features=features,
        final_score=78.0,
        strong_pillars=3,
        risk_penalty=5.0,
        portfolio_penalty=0.0,
        data_stale=False,
    )

    assert snapshot.ticker == "AAA"
    assert snapshot.features.feature_version == "market-v1"
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```powershell
python -m pytest tests/unit/test_config.py tests/unit/test_models.py -q
```

Expected: failures show missing `catalyst_radar.core.config` and `catalyst_radar.core.models`.

- [ ] **Step 4: Implement `config.py`**

Create `src/catalyst_radar/core/config.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from os import environ
from typing import Mapping


def _bool(value: str | bool | None, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _float(env: Mapping[str, str], key: str, default: float) -> float:
    raw = env.get(key)
    return default if raw is None or raw == "" else float(raw)


@dataclass(frozen=True)
class AppConfig:
    environment: str = "local"
    database_url: str = "sqlite:///data/local/catalyst_radar.db"
    log_level: str = "INFO"
    price_min: float = 5
    market_cap_min: float = 300_000_000
    avg_dollar_volume_min: float = 10_000_000
    risk_per_trade_pct: float = 0.005
    max_single_name_pct: float = 0.08
    max_sector_pct: float = 0.30
    max_theme_pct: float = 0.35
    enable_premium_llm: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "AppConfig":
        source = environ if env is None else env
        return cls(
            environment=source.get("CATALYST_ENV", "local"),
            database_url=source.get(
                "CATALYST_DATABASE_URL", "sqlite:///data/local/catalyst_radar.db"
            ),
            log_level=source.get("CATALYST_LOG_LEVEL", "INFO"),
            price_min=_float(source, "CATALYST_PRICE_MIN", 5),
            market_cap_min=_float(source, "CATALYST_MARKET_CAP_MIN", 300_000_000),
            avg_dollar_volume_min=_float(
                source, "CATALYST_AVG_DOLLAR_VOLUME_MIN", 10_000_000
            ),
            risk_per_trade_pct=_float(source, "CATALYST_RISK_PER_TRADE_PCT", 0.005),
            max_single_name_pct=_float(source, "CATALYST_MAX_SINGLE_NAME_PCT", 0.08),
            max_sector_pct=_float(source, "CATALYST_MAX_SECTOR_PCT", 0.30),
            max_theme_pct=_float(source, "CATALYST_MAX_THEME_PCT", 0.35),
            enable_premium_llm=_bool(source.get("CATALYST_ENABLE_PREMIUM_LLM"), False),
        )
```

- [ ] **Step 5: Implement `models.py`**

Create `src/catalyst_radar/core/models.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any


class ActionState(str, Enum):
    NO_ACTION = "NoAction"
    RESEARCH_ONLY = "ResearchOnly"
    ADD_TO_WATCHLIST = "AddToWatchlist"
    WARNING = "Warning"
    ELIGIBLE_FOR_MANUAL_BUY_REVIEW = "EligibleForManualBuyReview"
    BLOCKED = "Blocked"
    THESIS_WEAKENING = "ThesisWeakening"
    EXIT_INVALIDATE_REVIEW = "ExitInvalidateReview"


@dataclass(frozen=True)
class Security:
    ticker: str
    name: str
    exchange: str
    sector: str
    industry: str
    market_cap: float
    avg_dollar_volume_20d: float
    has_options: bool
    is_active: bool
    updated_at: datetime


@dataclass(frozen=True)
class DailyBar:
    ticker: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: float
    adjusted: bool
    provider: str
    source_ts: datetime
    available_at: datetime


@dataclass(frozen=True)
class MarketFeatures:
    ticker: str
    as_of: datetime
    ret_5d: float
    ret_20d: float
    rs_20_sector: float
    rs_60_spy: float
    near_52w_high: float
    ma_regime: float
    rel_volume_5d: float
    dollar_volume_z: float
    atr_pct: float
    extension_20d: float
    liquidity_score: float
    feature_version: str


@dataclass(frozen=True)
class PortfolioImpact:
    ticker: str
    single_name_after_pct: float
    sector_after_pct: float
    theme_after_pct: float
    portfolio_penalty: float
    hard_blocks: tuple[str, ...] = ()


@dataclass(frozen=True)
class CandidateSnapshot:
    ticker: str
    as_of: datetime
    features: MarketFeatures
    final_score: float
    strong_pillars: int
    risk_penalty: float
    portfolio_penalty: float
    data_stale: bool
    entry_zone: tuple[float, float] | None = None
    invalidation_price: float | None = None
    reward_risk: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PolicyResult:
    state: ActionState
    hard_blocks: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    missing_trade_plan: tuple[str, ...] = ()

    @property
    def is_blocked(self) -> bool:
        return self.state == ActionState.BLOCKED
```

- [ ] **Step 6: Run tests and verify pass**

Run:

```powershell
python -m pytest tests/unit/test_config.py tests/unit/test_models.py -q
```

Expected: all tests pass.

- [ ] **Step 7: Run lint and commit**

Run:

```powershell
python -m ruff check src tests
git add src/catalyst_radar/core tests/unit/test_config.py tests/unit/test_models.py
git commit -m "feat: add core config and domain models"
```

Expected: lint passes and commit succeeds.

## Task 3: Storage Schema and Repositories

**Files:**

- Create: `src/catalyst_radar/storage/schema.py`
- Create: `src/catalyst_radar/storage/db.py`
- Create: `src/catalyst_radar/storage/repositories.py`
- Create: `sql/migrations/001_initial.sql`
- Test: `tests/integration/test_csv_ingest.py`

- [ ] **Step 1: Write failing repository integration test**

Create `tests/integration/test_csv_ingest.py`:

```python
from datetime import UTC, datetime

from sqlalchemy import create_engine

from catalyst_radar.core.models import DailyBar, Security
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository


def test_repository_round_trips_security_and_bars() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = MarketRepository(engine)

    updated_at = datetime(2026, 5, 8, 20, tzinfo=UTC)
    repo.upsert_securities(
        [
            Security(
                ticker="AAA",
                name="Alpha Analytics",
                exchange="NASDAQ",
                sector="Technology",
                industry="Software",
                market_cap=5_000_000_000,
                avg_dollar_volume_20d=50_000_000,
                has_options=True,
                is_active=True,
                updated_at=updated_at,
            )
        ]
    )

    repo.upsert_daily_bars(
        [
            DailyBar(
                ticker="AAA",
                date=updated_at.date(),
                open=100,
                high=110,
                low=99,
                close=109,
                volume=1_500_000,
                vwap=108,
                adjusted=True,
                provider="sample",
                source_ts=updated_at,
                available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
            )
        ]
    )

    securities = repo.list_active_securities()
    bars = repo.daily_bars("AAA", end=updated_at.date(), lookback=10)

    assert [security.ticker for security in securities] == ["AAA"]
    assert len(bars) == 1
    assert bars[0].close == 109
```

- [ ] **Step 2: Run test and verify failure**

Run:

```powershell
python -m pytest tests/integration/test_csv_ingest.py -q
```

Expected: failure shows missing storage modules.

- [ ] **Step 3: Implement SQLAlchemy schema**

Create `src/catalyst_radar/storage/schema.py`:

```python
from __future__ import annotations

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
)

metadata = MetaData()

securities = Table(
    "securities",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("name", Text, nullable=False),
    Column("exchange", String, nullable=False),
    Column("sector", String, nullable=False),
    Column("industry", String, nullable=False),
    Column("market_cap", Float, nullable=False),
    Column("avg_dollar_volume_20d", Float, nullable=False),
    Column("has_options", Boolean, nullable=False, default=False),
    Column("is_active", Boolean, nullable=False, default=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

daily_bars = Table(
    "daily_bars",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("date", Date, primary_key=True),
    Column("provider", String, primary_key=True),
    Column("open", Float, nullable=False),
    Column("high", Float, nullable=False),
    Column("low", Float, nullable=False),
    Column("close", Float, nullable=False),
    Column("volume", Integer, nullable=False),
    Column("vwap", Float, nullable=False),
    Column("adjusted", Boolean, nullable=False, default=True),
    Column("source_ts", DateTime(timezone=True), nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
)

signal_features = Table(
    "signal_features",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("as_of", DateTime(timezone=True), primary_key=True),
    Column("feature_version", String, primary_key=True),
    Column("price_strength", Float, nullable=False),
    Column("volume_score", Float, nullable=False),
    Column("liquidity_score", Float, nullable=False),
    Column("risk_penalty", Float, nullable=False),
    Column("portfolio_penalty", Float, nullable=False),
    Column("final_score", Float, nullable=False),
    Column("payload", JSON, nullable=False),
)

candidate_states = Table(
    "candidate_states",
    metadata,
    Column("id", String, primary_key=True),
    Column("ticker", String, nullable=False),
    Column("as_of", DateTime(timezone=True), nullable=False),
    Column("state", String, nullable=False),
    Column("previous_state", String),
    Column("final_score", Float, nullable=False),
    Column("score_delta_5d", Float, nullable=False, default=0),
    Column("hard_blocks", JSON, nullable=False),
    Column("transition_reasons", JSON, nullable=False),
    Column("feature_version", String, nullable=False),
    Column("policy_version", String, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

holdings_snapshots = Table(
    "holdings_snapshots",
    metadata,
    Column("ticker", String, primary_key=True),
    Column("as_of", DateTime(timezone=True), primary_key=True),
    Column("shares", Float, nullable=False),
    Column("market_value", Float, nullable=False),
    Column("sector", String, nullable=False),
    Column("theme", String, nullable=False),
)
```

- [ ] **Step 4: Implement DB helper**

Create `src/catalyst_radar/storage/db.py`:

```python
from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine

from catalyst_radar.storage.schema import metadata


def engine_from_url(database_url: str) -> Engine:
    if database_url.startswith("sqlite:///"):
        db_path = Path(database_url.removeprefix("sqlite:///"))
        if str(db_path) != ":memory:":
            db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(database_url, future=True)


def create_schema(engine: Engine) -> None:
    metadata.create_all(engine)
```

- [ ] **Step 5: Implement repositories**

Create `src/catalyst_radar/storage/repositories.py`:

```python
from __future__ import annotations

from datetime import date, datetime
from typing import Iterable

from sqlalchemy import Engine, delete, insert, select

from catalyst_radar.core.models import DailyBar, Security
from catalyst_radar.storage.schema import daily_bars, securities


class MarketRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def upsert_securities(self, rows: Iterable[Security]) -> None:
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(delete(securities).where(securities.c.ticker == row.ticker))
                conn.execute(
                    insert(securities).values(
                        ticker=row.ticker,
                        name=row.name,
                        exchange=row.exchange,
                        sector=row.sector,
                        industry=row.industry,
                        market_cap=row.market_cap,
                        avg_dollar_volume_20d=row.avg_dollar_volume_20d,
                        has_options=row.has_options,
                        is_active=row.is_active,
                        updated_at=row.updated_at,
                    )
                )

    def upsert_daily_bars(self, rows: Iterable[DailyBar]) -> None:
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(
                    delete(daily_bars).where(
                        daily_bars.c.ticker == row.ticker,
                        daily_bars.c.date == row.date,
                        daily_bars.c.provider == row.provider,
                    )
                )
                conn.execute(
                    insert(daily_bars).values(
                        ticker=row.ticker,
                        date=row.date,
                        provider=row.provider,
                        open=row.open,
                        high=row.high,
                        low=row.low,
                        close=row.close,
                        volume=row.volume,
                        vwap=row.vwap,
                        adjusted=row.adjusted,
                        source_ts=row.source_ts,
                        available_at=row.available_at,
                    )
                )

    def list_active_securities(self) -> list[Security]:
        stmt = select(securities).where(securities.c.is_active.is_(True)).order_by(securities.c.ticker)
        with self.engine.connect() as conn:
            return [
                Security(
                    ticker=row.ticker,
                    name=row.name,
                    exchange=row.exchange,
                    sector=row.sector,
                    industry=row.industry,
                    market_cap=row.market_cap,
                    avg_dollar_volume_20d=row.avg_dollar_volume_20d,
                    has_options=row.has_options,
                    is_active=row.is_active,
                    updated_at=_as_datetime(row.updated_at),
                )
                for row in conn.execute(stmt)
            ]

    def daily_bars(self, ticker: str, end: date, lookback: int) -> list[DailyBar]:
        stmt = (
            select(daily_bars)
            .where(daily_bars.c.ticker == ticker, daily_bars.c.date <= end)
            .order_by(daily_bars.c.date.desc())
            .limit(lookback)
        )
        with self.engine.connect() as conn:
            rows = list(conn.execute(stmt))
        return [
            DailyBar(
                ticker=row.ticker,
                date=row.date,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
                vwap=row.vwap,
                adjusted=row.adjusted,
                provider=row.provider,
                source_ts=_as_datetime(row.source_ts),
                available_at=_as_datetime(row.available_at),
            )
            for row in reversed(rows)
        ]


def _as_datetime(value: datetime) -> datetime:
    return value
```

- [ ] **Step 6: Add Postgres migration SQL**

Create `sql/migrations/001_initial.sql`:

```sql
CREATE TABLE IF NOT EXISTS securities (
  ticker TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  exchange TEXT NOT NULL,
  sector TEXT NOT NULL,
  industry TEXT NOT NULL,
  market_cap DOUBLE PRECISION NOT NULL,
  avg_dollar_volume_20d DOUBLE PRECISION NOT NULL,
  has_options BOOLEAN NOT NULL DEFAULT FALSE,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_bars (
  ticker TEXT NOT NULL,
  date DATE NOT NULL,
  provider TEXT NOT NULL,
  open DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  low DOUBLE PRECISION NOT NULL,
  close DOUBLE PRECISION NOT NULL,
  volume BIGINT NOT NULL,
  vwap DOUBLE PRECISION NOT NULL,
  adjusted BOOLEAN NOT NULL DEFAULT TRUE,
  source_ts TIMESTAMPTZ NOT NULL,
  available_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (ticker, date, provider)
);

CREATE TABLE IF NOT EXISTS signal_features (
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  feature_version TEXT NOT NULL,
  price_strength DOUBLE PRECISION NOT NULL,
  volume_score DOUBLE PRECISION NOT NULL,
  liquidity_score DOUBLE PRECISION NOT NULL,
  risk_penalty DOUBLE PRECISION NOT NULL,
  portfolio_penalty DOUBLE PRECISION NOT NULL,
  final_score DOUBLE PRECISION NOT NULL,
  payload JSONB NOT NULL,
  PRIMARY KEY (ticker, as_of, feature_version)
);

CREATE TABLE IF NOT EXISTS candidate_states (
  id TEXT PRIMARY KEY,
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  state TEXT NOT NULL,
  previous_state TEXT,
  final_score DOUBLE PRECISION NOT NULL,
  score_delta_5d DOUBLE PRECISION NOT NULL DEFAULT 0,
  hard_blocks JSONB NOT NULL,
  transition_reasons JSONB NOT NULL,
  feature_version TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS holdings_snapshots (
  ticker TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  shares DOUBLE PRECISION NOT NULL,
  market_value DOUBLE PRECISION NOT NULL,
  sector TEXT NOT NULL,
  theme TEXT NOT NULL,
  PRIMARY KEY (ticker, as_of)
);
```

- [ ] **Step 7: Run storage tests**

Run:

```powershell
python -m pytest tests/integration/test_csv_ingest.py -q
python -m ruff check src tests
```

Expected: tests and lint pass.

- [ ] **Step 8: Commit storage**

Run:

```powershell
git add src/catalyst_radar/storage sql/migrations tests/integration/test_csv_ingest.py
git commit -m "feat: add storage schema and repositories"
```

Expected: commit succeeds.

## Task 4: CSV Connector and CLI

**Files:**

- Create: `src/catalyst_radar/connectors/csv_market.py`
- Create: `src/catalyst_radar/cli.py`
- Modify: `tests/integration/test_csv_ingest.py`

- [ ] **Step 1: Extend failing CSV ingest test**

Append this test to `tests/integration/test_csv_ingest.py`:

```python
from pathlib import Path

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv


def test_csv_connector_loads_fixture_rows() -> None:
    fixture_dir = Path("tests/fixtures")

    securities_rows = load_securities_csv(fixture_dir / "securities.csv")
    daily_bar_rows = load_daily_bars_csv(fixture_dir / "daily_bars.csv")

    assert securities_rows[0].ticker == "AAA"
    assert daily_bar_rows[0].provider == "sample"
    assert daily_bar_rows[0].available_at.isoformat().startswith("2026-05-01T21:00:00")
```

- [ ] **Step 2: Run test and verify failure**

Run:

```powershell
python -m pytest tests/integration/test_csv_ingest.py::test_csv_connector_loads_fixture_rows -q
```

Expected: failure shows missing `catalyst_radar.connectors.csv_market`.

- [ ] **Step 3: Implement CSV connector**

Create `src/catalyst_radar/connectors/csv_market.py`:

```python
from __future__ import annotations

from pathlib import Path

import pandas as pd

from catalyst_radar.core.models import DailyBar, Security


def load_securities_csv(path: str | Path) -> list[Security]:
    frame = pd.read_csv(path)
    rows: list[Security] = []
    for record in frame.to_dict(orient="records"):
        rows.append(
            Security(
                ticker=str(record["ticker"]).upper(),
                name=str(record["name"]),
                exchange=str(record["exchange"]),
                sector=str(record["sector"]),
                industry=str(record["industry"]),
                market_cap=float(record["market_cap"]),
                avg_dollar_volume_20d=float(record["avg_dollar_volume_20d"]),
                has_options=_to_bool(record["has_options"]),
                is_active=_to_bool(record["is_active"]),
                updated_at=pd.Timestamp(record["updated_at"]).to_pydatetime(),
            )
        )
    return rows


def load_daily_bars_csv(path: str | Path) -> list[DailyBar]:
    frame = pd.read_csv(path)
    rows: list[DailyBar] = []
    for record in frame.to_dict(orient="records"):
        rows.append(
            DailyBar(
                ticker=str(record["ticker"]).upper(),
                date=pd.Timestamp(record["date"]).date(),
                open=float(record["open"]),
                high=float(record["high"]),
                low=float(record["low"]),
                close=float(record["close"]),
                volume=int(record["volume"]),
                vwap=float(record["vwap"]),
                adjusted=_to_bool(record["adjusted"]),
                provider=str(record["provider"]),
                source_ts=pd.Timestamp(record["source_ts"]).to_pydatetime(),
                available_at=pd.Timestamp(record["available_at"]).to_pydatetime(),
            )
        )
    return rows


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
```

- [ ] **Step 4: Implement CLI**

Create `src/catalyst_radar/cli.py`:

```python
from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.core.config import AppConfig
from catalyst_radar.storage.db import create_schema, engine_from_url
from catalyst_radar.storage.repositories import MarketRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="catalyst-radar")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db")

    ingest = subparsers.add_parser("ingest-csv")
    ingest.add_argument("--securities", type=Path, required=True)
    ingest.add_argument("--daily-bars", type=Path, required=True)
    ingest.add_argument("--holdings", type=Path)

    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv(".env.local")
    args = build_parser().parse_args(argv)
    config = AppConfig.from_env()
    engine = engine_from_url(config.database_url)

    if args.command == "init-db":
        create_schema(engine)
        print("initialized database")
        return 0

    if args.command == "ingest-csv":
        create_schema(engine)
        repo = MarketRepository(engine)
        securities = load_securities_csv(args.securities)
        daily_bars = load_daily_bars_csv(args.daily_bars)
        repo.upsert_securities(securities)
        repo.upsert_daily_bars(daily_bars)
        print(f"ingested securities={len(securities)} daily_bars={len(daily_bars)}")
        return 0

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Run connector tests**

Run:

```powershell
python -m pytest tests/integration/test_csv_ingest.py -q
python -m ruff check src tests
```

Expected: tests and lint pass.

- [ ] **Step 6: Smoke test CLI**

Run:

```powershell
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
catalyst-radar init-db
catalyst-radar ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
```

Expected output includes:

```text
initialized database
ingested securities=6 daily_bars=36
```

- [ ] **Step 7: Commit connector and CLI**

Run:

```powershell
git add src/catalyst_radar/connectors src/catalyst_radar/cli.py tests/integration/test_csv_ingest.py
git commit -m "feat: add csv ingest connector"
```

Expected: commit succeeds.

## Task 5: Market Feature Engine

**Files:**

- Create: `src/catalyst_radar/features/market.py`
- Test: `tests/unit/test_market_features.py`

- [ ] **Step 1: Write failing feature tests**

Create `tests/unit/test_market_features.py`:

```python
from datetime import UTC, datetime

import pandas as pd

from catalyst_radar.features.market import compute_market_features


def test_market_features_score_strong_relative_move() -> None:
    as_of = datetime(2026, 5, 8, 21, tzinfo=UTC)
    ticker_bars = _bars("AAA", [94, 98, 100, 102, 105, 109], [500000, 700000, 800000, 850000, 1100000, 1500000])
    spy_bars = _bars("SPY", [504, 507, 506, 509, 510, 512], [70000000, 72000000, 71000000, 73000000, 74000000, 76000000])
    sector_bars = _bars("XLK", [202, 205, 206, 209, 211, 215], [10000000, 11000000, 10500000, 12000000, 13000000, 14000000])

    features = compute_market_features("AAA", as_of, ticker_bars, spy_bars, sector_bars)

    assert features.ticker == "AAA"
    assert features.ret_5d > 0.10
    assert features.rs_20_sector > 50
    assert features.rs_60_spy > 50
    assert features.rel_volume_5d > 1
    assert features.liquidity_score == 100


def test_market_features_penalize_illiquid_name() -> None:
    as_of = datetime(2026, 5, 8, 21, tzinfo=UTC)
    ticker_bars = _bars("CCC", [8.1, 8.0, 8.0, 7.9, 7.8, 7.6], [250000, 180000, 160000, 150000, 140000, 130000])
    spy_bars = _bars("SPY", [504, 507, 506, 509, 510, 512], [70000000, 72000000, 71000000, 73000000, 74000000, 76000000])
    sector_bars = _bars("XLK", [202, 205, 206, 209, 211, 215], [10000000, 11000000, 10500000, 12000000, 13000000, 14000000])

    features = compute_market_features("CCC", as_of, ticker_bars, spy_bars, sector_bars)

    assert features.ret_5d < 0
    assert features.liquidity_score < 50


def _bars(ticker: str, closes: list[float], volumes: list[int]) -> pd.DataFrame:
    dates = pd.date_range("2026-05-01", periods=len(closes), freq="B")
    return pd.DataFrame(
        {
            "ticker": ticker,
            "date": dates,
            "open": closes,
            "high": [value * 1.01 for value in closes],
            "low": [value * 0.99 for value in closes],
            "close": closes,
            "volume": volumes,
            "vwap": closes,
        }
    )
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```powershell
python -m pytest tests/unit/test_market_features.py -q
```

Expected: failure shows missing feature module.

- [ ] **Step 3: Implement market features**

Create `src/catalyst_radar/features/market.py`:

```python
from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from catalyst_radar.core.models import MarketFeatures

FEATURE_VERSION = "market-v1"


def compute_market_features(
    ticker: str,
    as_of: datetime,
    ticker_bars: pd.DataFrame,
    spy_bars: pd.DataFrame,
    sector_bars: pd.DataFrame,
) -> MarketFeatures:
    ticker_bars = ticker_bars.sort_values("date").reset_index(drop=True)
    spy_bars = spy_bars.sort_values("date").reset_index(drop=True)
    sector_bars = sector_bars.sort_values("date").reset_index(drop=True)

    close = ticker_bars["close"].astype(float)
    high = ticker_bars["high"].astype(float)
    low = ticker_bars["low"].astype(float)
    volume = ticker_bars["volume"].astype(float)

    ret_5d = _return(close, 5)
    ret_20d = _return(close, min(20, len(close) - 1))
    sector_ret = _return(sector_bars["close"].astype(float), min(20, len(sector_bars) - 1))
    spy_ret = _return(spy_bars["close"].astype(float), min(20, len(spy_bars) - 1))

    rs_20_sector = _bounded_score(50 + ((ret_20d - sector_ret) * 500))
    rs_60_spy = _bounded_score(50 + ((ret_20d - spy_ret) * 500))
    near_52w_high = float(close.iloc[-1] / max(high.max(), 0.01))
    ma_regime = _ma_regime(close)
    rel_volume_5d = float(volume.tail(5).mean() / max(volume.median(), 1))
    dollar_volume = close * volume
    dollar_volume_z = _zscore_last(dollar_volume)
    atr_pct = _atr_pct(high, low, close)
    extension_20d = float((close.iloc[-1] / max(close.tail(min(20, len(close))).mean(), 0.01)) - 1)
    liquidity_score = _liquidity_score(float(dollar_volume.tail(min(20, len(dollar_volume))).mean()))

    return MarketFeatures(
        ticker=ticker,
        as_of=as_of,
        ret_5d=ret_5d,
        ret_20d=ret_20d,
        rs_20_sector=rs_20_sector,
        rs_60_spy=rs_60_spy,
        near_52w_high=near_52w_high,
        ma_regime=ma_regime,
        rel_volume_5d=rel_volume_5d,
        dollar_volume_z=dollar_volume_z,
        atr_pct=atr_pct,
        extension_20d=extension_20d,
        liquidity_score=liquidity_score,
        feature_version=FEATURE_VERSION,
    )


def _return(close: pd.Series, periods: int) -> float:
    if len(close) <= periods or periods <= 0:
        return 0.0
    return float((close.iloc[-1] / close.iloc[-1 - periods]) - 1)


def _bounded_score(value: float) -> float:
    return float(max(0, min(100, value)))


def _ma_regime(close: pd.Series) -> float:
    short = close.tail(min(5, len(close))).mean()
    long = close.tail(min(20, len(close))).mean()
    if short > long and close.iloc[-1] >= short:
        return 90.0
    if close.iloc[-1] >= long:
        return 65.0
    return 30.0


def _zscore_last(values: pd.Series) -> float:
    if len(values) < 3:
        return 0.0
    std = float(values.std(ddof=0))
    if std == 0:
        return 0.0
    return float((values.iloc[-1] - values.mean()) / std)


def _atr_pct(high: pd.Series, low: pd.Series, close: pd.Series) -> float:
    previous_close = close.shift(1).fillna(close.iloc[0])
    true_range = pd.concat(
        [(high - low), (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    atr = float(true_range.tail(min(14, len(true_range))).mean())
    return atr / max(float(close.iloc[-1]), 0.01)


def _liquidity_score(avg_dollar_volume: float) -> float:
    if avg_dollar_volume >= 10_000_000:
        return 100.0
    if avg_dollar_volume >= 5_000_000:
        return 60.0
    if avg_dollar_volume >= 2_000_000:
        return 35.0
    return 10.0
```

- [ ] **Step 4: Run feature tests**

Run:

```powershell
python -m pytest tests/unit/test_market_features.py -q
python -m ruff check src tests
```

Expected: tests and lint pass.

- [ ] **Step 5: Commit feature engine**

Run:

```powershell
git add src/catalyst_radar/features tests/unit/test_market_features.py
git commit -m "feat: add deterministic market features"
```

Expected: commit succeeds.

## Task 6: Scoring, Policy, and Portfolio Gates

**Files:**

- Create: `src/catalyst_radar/scoring/score.py`
- Create: `src/catalyst_radar/scoring/policy.py`
- Create: `src/catalyst_radar/portfolio/risk.py`
- Test: `tests/unit/test_score.py`
- Test: `tests/unit/test_policy.py`
- Test: `tests/unit/test_portfolio.py`

- [ ] **Step 1: Write failing score tests**

Create `tests/unit/test_score.py`:

```python
from datetime import UTC, datetime

from catalyst_radar.core.models import MarketFeatures
from catalyst_radar.scoring.score import score_market_features


def test_score_market_features_rewards_strength_and_volume() -> None:
    features = MarketFeatures(
        ticker="AAA",
        as_of=datetime(2026, 5, 8, 21, tzinfo=UTC),
        ret_5d=0.12,
        ret_20d=0.15,
        rs_20_sector=82,
        rs_60_spy=85,
        near_52w_high=0.99,
        ma_regime=90,
        rel_volume_5d=2.0,
        dollar_volume_z=2.0,
        atr_pct=0.04,
        extension_20d=0.08,
        liquidity_score=100,
        feature_version="market-v1",
    )

    result = score_market_features(features, portfolio_penalty=0)

    assert result.final_score >= 72
    assert result.strong_pillars >= 3
    assert result.risk_penalty < 12
```

- [ ] **Step 2: Write failing policy tests**

Create `tests/unit/test_policy.py`:

```python
from datetime import UTC, datetime

from catalyst_radar.core.models import ActionState, CandidateSnapshot, MarketFeatures
from catalyst_radar.scoring.policy import evaluate_policy


def test_policy_blocks_stale_data() -> None:
    candidate = _candidate(final_score=90, data_stale=True)

    result = evaluate_policy(candidate)

    assert result.state == ActionState.BLOCKED
    assert "core_data_stale" in result.hard_blocks


def test_policy_uses_watchlist_state_for_mid_scores() -> None:
    candidate = _candidate(final_score=65, strong_pillars=2)

    result = evaluate_policy(candidate)

    assert result.state == ActionState.ADD_TO_WATCHLIST


def test_policy_requires_trade_plan_for_buy_review() -> None:
    candidate = _candidate(final_score=88, strong_pillars=3, entry_zone=None, invalidation_price=None)

    result = evaluate_policy(candidate)

    assert result.state == ActionState.WARNING
    assert "missing_entry_zone" in result.missing_trade_plan
    assert "missing_invalidation" in result.missing_trade_plan


def test_policy_allows_manual_buy_review_when_all_gates_pass() -> None:
    candidate = _candidate(
        final_score=88,
        strong_pillars=3,
        entry_zone=(100, 103),
        invalidation_price=94.5,
        reward_risk=2.5,
    )

    result = evaluate_policy(candidate)

    assert result.state == ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW


def _candidate(
    final_score: float,
    strong_pillars: int = 3,
    data_stale: bool = False,
    entry_zone: tuple[float, float] | None = (100, 103),
    invalidation_price: float | None = 94.5,
    reward_risk: float = 2.5,
) -> CandidateSnapshot:
    as_of = datetime(2026, 5, 8, 21, tzinfo=UTC)
    return CandidateSnapshot(
        ticker="AAA",
        as_of=as_of,
        features=MarketFeatures(
            ticker="AAA",
            as_of=as_of,
            ret_5d=0.12,
            ret_20d=0.15,
            rs_20_sector=82,
            rs_60_spy=85,
            near_52w_high=0.99,
            ma_regime=90,
            rel_volume_5d=2.0,
            dollar_volume_z=2.0,
            atr_pct=0.04,
            extension_20d=0.08,
            liquidity_score=100,
            feature_version="market-v1",
        ),
        final_score=final_score,
        strong_pillars=strong_pillars,
        risk_penalty=5,
        portfolio_penalty=0,
        data_stale=data_stale,
        entry_zone=entry_zone,
        invalidation_price=invalidation_price,
        reward_risk=reward_risk,
    )
```

- [ ] **Step 3: Write failing portfolio tests**

Create `tests/unit/test_portfolio.py`:

```python
from catalyst_radar.portfolio.risk import PortfolioPolicy, compute_position_size, evaluate_portfolio_impact


def test_position_size_uses_max_loss_budget() -> None:
    size = compute_position_size(
        portfolio_value=100_000,
        risk_per_trade_pct=0.005,
        entry_price=100,
        invalidation_price=95,
        max_single_name_pct=0.08,
    )

    assert size.shares == 100
    assert size.position_value == 10_000
    assert size.capped_by_single_name is True


def test_portfolio_impact_blocks_sector_overexposure() -> None:
    impact = evaluate_portfolio_impact(
        ticker="AAA",
        sector="Technology",
        theme="ai_infrastructure",
        proposed_position_value=10_000,
        portfolio_value=100_000,
        existing_sector_value=25_000,
        existing_theme_value=5_000,
        policy=PortfolioPolicy(max_sector_pct=0.30),
    )

    assert "sector_exposure_hard_block" in impact.hard_blocks
    assert impact.portfolio_penalty > 0
```

- [ ] **Step 4: Run tests and verify failure**

Run:

```powershell
python -m pytest tests/unit/test_score.py tests/unit/test_policy.py tests/unit/test_portfolio.py -q
```

Expected: failures show missing scoring and portfolio modules.

- [ ] **Step 5: Implement scoring**

Create `src/catalyst_radar/scoring/score.py`:

```python
from __future__ import annotations

from dataclasses import dataclass

from catalyst_radar.core.models import CandidateSnapshot, MarketFeatures


@dataclass(frozen=True)
class ScoreResult:
    final_score: float
    strong_pillars: int
    risk_penalty: float
    price_strength: float
    volume_score: float
    liquidity_score: float


def score_market_features(features: MarketFeatures, portfolio_penalty: float) -> ScoreResult:
    price_strength = _bounded(
        0.35 * features.rs_20_sector
        + 0.35 * features.rs_60_spy
        + 0.20 * features.ma_regime
        + 0.10 * (features.near_52w_high * 100)
    )
    volume_score = _bounded(50 + (features.rel_volume_5d - 1) * 25 + features.dollar_volume_z * 8)
    risk_penalty = _risk_penalty(features)
    strong_pillars = sum(
        score >= 70 for score in (price_strength, volume_score, features.liquidity_score)
    )
    final_score = _bounded(
        0.55 * price_strength
        + 0.20 * volume_score
        + 0.15 * features.liquidity_score
        + 0.10 * features.ma_regime
        - risk_penalty
        - portfolio_penalty
    )
    return ScoreResult(
        final_score=final_score,
        strong_pillars=strong_pillars,
        risk_penalty=risk_penalty,
        price_strength=price_strength,
        volume_score=volume_score,
        liquidity_score=features.liquidity_score,
    )


def candidate_from_features(
    features: MarketFeatures,
    portfolio_penalty: float,
    data_stale: bool,
    entry_zone: tuple[float, float] | None,
    invalidation_price: float | None,
    reward_risk: float,
) -> CandidateSnapshot:
    score = score_market_features(features, portfolio_penalty)
    return CandidateSnapshot(
        ticker=features.ticker,
        as_of=features.as_of,
        features=features,
        final_score=score.final_score,
        strong_pillars=score.strong_pillars,
        risk_penalty=score.risk_penalty,
        portfolio_penalty=portfolio_penalty,
        data_stale=data_stale,
        entry_zone=entry_zone,
        invalidation_price=invalidation_price,
        reward_risk=reward_risk,
        metadata={
            "price_strength": score.price_strength,
            "volume_score": score.volume_score,
            "liquidity_score": score.liquidity_score,
        },
    )


def _risk_penalty(features: MarketFeatures) -> float:
    penalty = 0.0
    if features.extension_20d > 0.15:
        penalty += 6
    if features.atr_pct > 0.08:
        penalty += 6
    if features.liquidity_score < 50:
        penalty += 12
    return penalty


def _bounded(value: float) -> float:
    return float(max(0, min(100, value)))
```

- [ ] **Step 6: Implement policy**

Create `src/catalyst_radar/scoring/policy.py`:

```python
from __future__ import annotations

from catalyst_radar.core.models import ActionState, CandidateSnapshot, PolicyResult

POLICY_VERSION = "policy-v1"


def evaluate_policy(candidate: CandidateSnapshot) -> PolicyResult:
    hard_blocks: list[str] = []
    reasons: list[str] = []

    if candidate.data_stale:
        hard_blocks.append("core_data_stale")
        reasons.append("Core market data is stale.")
    if candidate.features.liquidity_score < 50:
        hard_blocks.append("liquidity_hard_block")
        reasons.append("Liquidity score is below the hard-block threshold.")
    if candidate.risk_penalty >= 20:
        hard_blocks.append("risk_penalty_hard_block")
        reasons.append("Risk penalty is too high.")
    if candidate.portfolio_penalty >= 20:
        hard_blocks.append("portfolio_exposure_hard_block")
        reasons.append("Portfolio exposure exceeds configured limits.")

    if hard_blocks:
        return PolicyResult(
            state=ActionState.BLOCKED,
            hard_blocks=tuple(hard_blocks),
            reasons=tuple(reasons),
        )

    missing_trade_plan = _missing_trade_plan(candidate)

    if candidate.final_score >= 85 and candidate.strong_pillars >= 3 and not missing_trade_plan:
        return PolicyResult(
            state=ActionState.ELIGIBLE_FOR_MANUAL_BUY_REVIEW,
            reasons=("All deterministic gates pass.",),
        )
    if candidate.final_score >= 72:
        return PolicyResult(
            state=ActionState.WARNING,
            reasons=("Multiple deterministic pillars are improving.",),
            missing_trade_plan=missing_trade_plan,
        )
    if candidate.final_score >= 60:
        return PolicyResult(
            state=ActionState.ADD_TO_WATCHLIST,
            reasons=("Candidate is worth monitoring.",),
            missing_trade_plan=missing_trade_plan,
        )
    if candidate.final_score >= 50:
        return PolicyResult(
            state=ActionState.RESEARCH_ONLY,
            reasons=("Candidate has some signal but insufficient evidence.",),
            missing_trade_plan=missing_trade_plan,
        )
    return PolicyResult(state=ActionState.NO_ACTION, reasons=("Signal below threshold.",))


def _missing_trade_plan(candidate: CandidateSnapshot) -> tuple[str, ...]:
    missing: list[str] = []
    if candidate.entry_zone is None:
        missing.append("missing_entry_zone")
    if candidate.invalidation_price is None:
        missing.append("missing_invalidation")
    if candidate.reward_risk < 2.0:
        missing.append("reward_risk_too_low")
    return tuple(missing)
```

- [ ] **Step 7: Implement portfolio risk**

Create `src/catalyst_radar/portfolio/risk.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from math import floor

from catalyst_radar.core.models import PortfolioImpact


@dataclass(frozen=True)
class PortfolioPolicy:
    risk_per_trade_pct: float = 0.005
    max_single_name_pct: float = 0.08
    max_sector_pct: float = 0.30
    max_theme_pct: float = 0.35


@dataclass(frozen=True)
class PositionSize:
    shares: int
    position_value: float
    risk_budget_dollars: float
    stop_distance: float
    capped_by_single_name: bool


def compute_position_size(
    portfolio_value: float,
    risk_per_trade_pct: float,
    entry_price: float,
    invalidation_price: float,
    max_single_name_pct: float,
) -> PositionSize:
    risk_budget_dollars = portfolio_value * risk_per_trade_pct
    stop_distance = abs(entry_price - invalidation_price)
    uncapped_shares = floor(risk_budget_dollars / stop_distance)
    uncapped_value = uncapped_shares * entry_price
    max_position_value = portfolio_value * max_single_name_pct
    if uncapped_value > max_position_value:
        shares = floor(max_position_value / entry_price)
        return PositionSize(
            shares=shares,
            position_value=shares * entry_price,
            risk_budget_dollars=risk_budget_dollars,
            stop_distance=stop_distance,
            capped_by_single_name=True,
        )
    return PositionSize(
        shares=uncapped_shares,
        position_value=uncapped_value,
        risk_budget_dollars=risk_budget_dollars,
        stop_distance=stop_distance,
        capped_by_single_name=False,
    )


def evaluate_portfolio_impact(
    ticker: str,
    sector: str,
    theme: str,
    proposed_position_value: float,
    portfolio_value: float,
    existing_sector_value: float,
    existing_theme_value: float,
    policy: PortfolioPolicy,
) -> PortfolioImpact:
    single_name_after_pct = proposed_position_value / portfolio_value
    sector_after_pct = (existing_sector_value + proposed_position_value) / portfolio_value
    theme_after_pct = (existing_theme_value + proposed_position_value) / portfolio_value
    hard_blocks: list[str] = []
    penalty = 0.0

    if single_name_after_pct > policy.max_single_name_pct:
        hard_blocks.append("single_name_exposure_hard_block")
        penalty += 20
    if sector_after_pct > policy.max_sector_pct:
        hard_blocks.append("sector_exposure_hard_block")
        penalty += 20
    if theme_after_pct > policy.max_theme_pct:
        hard_blocks.append("theme_exposure_hard_block")
        penalty += 20

    return PortfolioImpact(
        ticker=ticker,
        single_name_after_pct=single_name_after_pct,
        sector_after_pct=sector_after_pct,
        theme_after_pct=theme_after_pct,
        portfolio_penalty=penalty,
        hard_blocks=tuple(hard_blocks),
    )
```

- [ ] **Step 8: Run scoring and policy tests**

Run:

```powershell
python -m pytest tests/unit/test_score.py tests/unit/test_policy.py tests/unit/test_portfolio.py -q
python -m ruff check src tests
```

Expected: tests and lint pass.

- [ ] **Step 9: Commit scoring and policy**

Run:

```powershell
git add src/catalyst_radar/scoring src/catalyst_radar/portfolio tests/unit/test_score.py tests/unit/test_policy.py tests/unit/test_portfolio.py
git commit -m "feat: add scoring policy and portfolio gates"
```

Expected: commit succeeds.

## Task 7: Scanner Pipeline

**Files:**

- Create: `src/catalyst_radar/pipeline/scan.py`
- Modify: `src/catalyst_radar/storage/repositories.py`
- Modify: `src/catalyst_radar/cli.py`
- Test: `tests/integration/test_scan_pipeline.py`

- [ ] **Step 1: Write failing scanner test**

Create `tests/integration/test_scan_pipeline.py`:

```python
from datetime import date
from pathlib import Path

from sqlalchemy import create_engine

from catalyst_radar.connectors.csv_market import load_daily_bars_csv, load_securities_csv
from catalyst_radar.pipeline.scan import run_scan
from catalyst_radar.storage.db import create_schema
from catalyst_radar.storage.repositories import MarketRepository


def test_scan_pipeline_produces_candidate_states() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = MarketRepository(engine)
    fixture_dir = Path("tests/fixtures")
    repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))

    results = run_scan(repo, as_of=date(2026, 5, 8))

    states = {result.ticker: result.policy.state.value for result in results}
    assert states["AAA"] in {"AddToWatchlist", "Warning", "EligibleForManualBuyReview"}
    assert states["CCC"] == "Blocked"
```

- [ ] **Step 2: Run test and verify failure**

Run:

```powershell
python -m pytest tests/integration/test_scan_pipeline.py -q
```

Expected: failure shows missing scanner pipeline.

- [ ] **Step 3: Implement scanner pipeline**

Create `src/catalyst_radar/pipeline/scan.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

import pandas as pd

from catalyst_radar.core.models import CandidateSnapshot, PolicyResult
from catalyst_radar.features.market import compute_market_features
from catalyst_radar.scoring.policy import evaluate_policy
from catalyst_radar.scoring.score import candidate_from_features
from catalyst_radar.storage.repositories import MarketRepository


@dataclass(frozen=True)
class ScanResult:
    ticker: str
    candidate: CandidateSnapshot
    policy: PolicyResult


SECTOR_ETF = {
    "Technology": "XLK",
    "Industrials": "XLI",
}


def run_scan(repo: MarketRepository, as_of: date) -> list[ScanResult]:
    as_of_dt = datetime(as_of.year, as_of.month, as_of.day, 21, tzinfo=UTC)
    securities = repo.list_active_securities()
    spy = _bars_frame(repo, "SPY", as_of)
    results: list[ScanResult] = []

    for security in securities:
        if security.ticker in {"SPY", "XLK", "XLI"}:
            continue
        ticker_bars = _bars_frame(repo, security.ticker, as_of)
        if ticker_bars.empty:
            continue
        sector_ticker = SECTOR_ETF.get(security.sector, "SPY")
        sector_bars = _bars_frame(repo, sector_ticker, as_of)
        features = compute_market_features(security.ticker, as_of_dt, ticker_bars, spy, sector_bars)
        entry_price = float(ticker_bars["close"].iloc[-1])
        entry_zone = (entry_price * 0.98, entry_price * 1.01)
        invalidation = entry_price * 0.92
        candidate = candidate_from_features(
            features=features,
            portfolio_penalty=0,
            data_stale=_is_data_stale(ticker_bars, as_of),
            entry_zone=entry_zone,
            invalidation_price=invalidation,
            reward_risk=2.2,
        )
        policy = evaluate_policy(candidate)
        results.append(ScanResult(ticker=security.ticker, candidate=candidate, policy=policy))

    return sorted(results, key=lambda result: result.candidate.final_score, reverse=True)


def _bars_frame(repo: MarketRepository, ticker: str, as_of: date) -> pd.DataFrame:
    bars = repo.daily_bars(ticker, end=as_of, lookback=300)
    return pd.DataFrame([bar.__dict__ for bar in bars])


def _is_data_stale(frame: pd.DataFrame, as_of: date) -> bool:
    if frame.empty:
        return True
    last_date = pd.Timestamp(frame["date"].max()).date()
    return last_date < as_of
```

- [ ] **Step 4: Extend repository to save scan results**

Add methods to `MarketRepository` in `src/catalyst_radar/storage/repositories.py`:

```python
from datetime import UTC
from uuid import uuid4

from catalyst_radar.core.models import CandidateSnapshot, PolicyResult
from catalyst_radar.storage.schema import candidate_states, signal_features


    def save_scan_result(self, candidate: CandidateSnapshot, policy: PolicyResult) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                insert(signal_features).values(
                    ticker=candidate.ticker,
                    as_of=candidate.as_of,
                    feature_version=candidate.features.feature_version,
                    price_strength=float(candidate.metadata.get("price_strength", 0)),
                    volume_score=float(candidate.metadata.get("volume_score", 0)),
                    liquidity_score=candidate.features.liquidity_score,
                    risk_penalty=candidate.risk_penalty,
                    portfolio_penalty=candidate.portfolio_penalty,
                    final_score=candidate.final_score,
                    payload={
                        "features": candidate.features.__dict__,
                        "metadata": candidate.metadata,
                    },
                )
            )
            conn.execute(
                insert(candidate_states).values(
                    id=str(uuid4()),
                    ticker=candidate.ticker,
                    as_of=candidate.as_of,
                    state=policy.state.value,
                    previous_state=None,
                    final_score=candidate.final_score,
                    score_delta_5d=0,
                    hard_blocks=list(policy.hard_blocks),
                    transition_reasons={"reasons": list(policy.reasons)},
                    feature_version=candidate.features.feature_version,
                    policy_version="policy-v1",
                    created_at=datetime.now(UTC),
                )
            )
```

Adjust imports so the module remains lint-clean.

- [ ] **Step 5: Add scan command to CLI**

Modify `src/catalyst_radar/cli.py`:

```python
from datetime import date

from catalyst_radar.pipeline.scan import run_scan
```

Add parser:

```python
    scan = subparsers.add_parser("scan")
    scan.add_argument("--as-of", type=date.fromisoformat, required=True)
```

Add command branch before the unsupported command error:

```python
    if args.command == "scan":
        create_schema(engine)
        repo = MarketRepository(engine)
        results = run_scan(repo, as_of=args.as_of)
        for result in results:
            repo.save_scan_result(result.candidate, result.policy)
        print(f"scanned candidates={len(results)}")
        return 0
```

- [ ] **Step 6: Run scanner tests**

Run:

```powershell
python -m pytest tests/integration/test_scan_pipeline.py -q
python -m ruff check src tests
```

Expected: tests and lint pass.

- [ ] **Step 7: Smoke test full CLI flow**

Run:

```powershell
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
catalyst-radar init-db
catalyst-radar ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
catalyst-radar scan --as-of 2026-05-08
```

Expected output includes:

```text
initialized database
ingested securities=6 daily_bars=36
scanned candidates=3
```

- [ ] **Step 8: Commit scanner pipeline**

Run:

```powershell
git add src/catalyst_radar/pipeline src/catalyst_radar/storage/repositories.py src/catalyst_radar/cli.py tests/integration/test_scan_pipeline.py
git commit -m "feat: add deterministic scan pipeline"
```

Expected: commit succeeds.

## Task 8: Dashboard

**Files:**

- Create: `src/catalyst_radar/dashboard/data.py`
- Create: `apps/dashboard/Home.py`
- Test: add dashboard repository read test to `tests/integration/test_scan_pipeline.py`

- [ ] **Step 1: Add failing dashboard data test**

Append to `tests/integration/test_scan_pipeline.py`:

```python
from catalyst_radar.dashboard.data import load_candidate_rows


def test_dashboard_loads_candidate_rows() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    create_schema(engine)
    repo = MarketRepository(engine)
    fixture_dir = Path("tests/fixtures")
    repo.upsert_securities(load_securities_csv(fixture_dir / "securities.csv"))
    repo.upsert_daily_bars(load_daily_bars_csv(fixture_dir / "daily_bars.csv"))
    for result in run_scan(repo, as_of=date(2026, 5, 8)):
        repo.save_scan_result(result.candidate, result.policy)

    rows = load_candidate_rows(engine)

    assert rows
    assert {"ticker", "state", "final_score", "hard_blocks"}.issubset(rows[0])
```

- [ ] **Step 2: Run test and verify failure**

Run:

```powershell
python -m pytest tests/integration/test_scan_pipeline.py::test_dashboard_loads_candidate_rows -q
```

Expected: failure shows missing dashboard data module.

- [ ] **Step 3: Implement dashboard data helper**

Create `src/catalyst_radar/dashboard/data.py`:

```python
from __future__ import annotations

from sqlalchemy import Engine, select

from catalyst_radar.storage.schema import candidate_states


def load_candidate_rows(engine: Engine) -> list[dict[str, object]]:
    stmt = (
        select(candidate_states)
        .order_by(candidate_states.c.final_score.desc(), candidate_states.c.as_of.desc())
        .limit(200)
    )
    with engine.connect() as conn:
        return [dict(row._mapping) for row in conn.execute(stmt)]
```

- [ ] **Step 4: Implement Streamlit dashboard**

Create `apps/dashboard/Home.py`:

```python
from __future__ import annotations

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from catalyst_radar.core.config import AppConfig
from catalyst_radar.dashboard.data import load_candidate_rows
from catalyst_radar.storage.db import engine_from_url

load_dotenv(".env.local")

st.set_page_config(page_title="Catalyst Radar", layout="wide")
st.title("Catalyst Radar")
st.caption("Deterministic Phase 1 radar. No LLM calls are required for this view.")

config = AppConfig.from_env()
engine = engine_from_url(config.database_url)
rows = load_candidate_rows(engine)

if not rows:
    st.info("No candidate states found. Run ingest and scan first.")
    st.code(
        "catalyst-radar ingest-csv --securities data/sample/securities.csv "
        "--daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv\n"
        "catalyst-radar scan --as-of 2026-05-08",
        language="powershell",
    )
else:
    frame = pd.DataFrame(rows)
    left, right = st.columns([2, 1])
    with left:
        st.subheader("Candidates")
        st.dataframe(
            frame[["ticker", "state", "final_score", "hard_blocks", "as_of"]],
            use_container_width=True,
            hide_index=True,
        )
    with right:
        st.subheader("State Mix")
        st.bar_chart(frame["state"].value_counts())
```

- [ ] **Step 5: Run dashboard data tests**

Run:

```powershell
python -m pytest tests/integration/test_scan_pipeline.py -q
python -m ruff check src tests apps
```

Expected: tests and lint pass.

- [ ] **Step 6: Manually launch dashboard**

Run:

```powershell
streamlit run apps/dashboard/Home.py
```

Expected: browser opens a Streamlit app showing a candidate table and state mix after sample ingest and scan. If no data exists, app shows the exact ingest/scan commands.

- [ ] **Step 7: Commit dashboard**

Run:

```powershell
git add src/catalyst_radar/dashboard apps/dashboard tests/integration/test_scan_pipeline.py
git commit -m "feat: add phase 1 dashboard"
```

Expected: commit succeeds.

## Task 9: Point-in-Time Validation Skeleton

**Files:**

- Create: `src/catalyst_radar/validation/backtest.py`
- Test: `tests/unit/test_backtest.py`

- [ ] **Step 1: Write failing validation tests**

Create `tests/unit/test_backtest.py`:

```python
from datetime import UTC, datetime

import pytest

from catalyst_radar.validation.backtest import assert_available_at_or_before_decision, label_forward_return


def test_availability_check_accepts_past_available_record() -> None:
    assert_available_at_or_before_decision(
        available_at=datetime(2026, 5, 8, 21, tzinfo=UTC),
        decision_at=datetime(2026, 5, 9, 13, 30, tzinfo=UTC),
    )


def test_availability_check_rejects_future_record() -> None:
    with pytest.raises(ValueError, match="future leakage"):
        assert_available_at_or_before_decision(
            available_at=datetime(2026, 5, 9, 14, tzinfo=UTC),
            decision_at=datetime(2026, 5, 9, 13, 30, tzinfo=UTC),
        )


def test_forward_return_labels() -> None:
    labels = label_forward_return(entry_price=100, max_forward_price=126, sector_return=0.02)

    assert labels["target_10d_15"] is True
    assert labels["target_20d_25"] is True
    assert labels["sector_outperformance"] is True
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```powershell
python -m pytest tests/unit/test_backtest.py -q
```

Expected: failure shows missing validation module.

- [ ] **Step 3: Implement validation helpers**

Create `src/catalyst_radar/validation/backtest.py`:

```python
from __future__ import annotations

from datetime import datetime


def assert_available_at_or_before_decision(available_at: datetime, decision_at: datetime) -> None:
    if available_at > decision_at:
        raise ValueError(
            f"future leakage: available_at={available_at.isoformat()} "
            f"is after decision_at={decision_at.isoformat()}"
        )


def label_forward_return(
    entry_price: float,
    max_forward_price: float,
    sector_return: float,
) -> dict[str, bool]:
    forward_return = (max_forward_price / entry_price) - 1
    return {
        "target_10d_15": forward_return >= 0.15,
        "target_20d_25": forward_return >= 0.25,
        "target_60d_40": forward_return >= 0.40,
        "sector_outperformance": (forward_return - sector_return) >= 0.20,
    }
```

- [ ] **Step 4: Run validation tests**

Run:

```powershell
python -m pytest tests/unit/test_backtest.py -q
python -m ruff check src tests
```

Expected: tests and lint pass.

- [ ] **Step 5: Commit validation skeleton**

Run:

```powershell
git add src/catalyst_radar/validation tests/unit/test_backtest.py
git commit -m "feat: add point-in-time validation helpers"
```

Expected: commit succeeds.

## Task 10: Full Verification and Phase 1 Review

**Files:**

- Modify: `README.md`
- Create: `docs/phase-1-review.md`

- [ ] **Step 1: Run full automated verification**

Run:

```powershell
python -m pytest
python -m ruff check src tests apps
```

Expected: all tests and lint pass.

- [ ] **Step 2: Run end-to-end smoke flow**

Run:

```powershell
Remove-Item data/local/catalyst_radar.db -ErrorAction SilentlyContinue
$env:CATALYST_DATABASE_URL="sqlite:///data/local/catalyst_radar.db"
catalyst-radar init-db
catalyst-radar ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
catalyst-radar scan --as-of 2026-05-08
```

Expected output:

```text
initialized database
ingested securities=6 daily_bars=36
scanned candidates=3
```

- [ ] **Step 3: Start dashboard for manual review**

Run:

```powershell
streamlit run apps/dashboard/Home.py
```

Expected: dashboard shows AAA, BBB, and CCC candidate rows. CCC is blocked because liquidity is below threshold.

- [ ] **Step 4: Create `docs/phase-1-review.md`**

Write:

````markdown
# Phase 1 Review

Date: 2026-05-09

## What works

- Local CSV securities ingest.
- Local CSV daily bar ingest.
- SQLite local database initialization.
- Deterministic market feature computation.
- Score and policy state assignment.
- Liquidity hard block.
- Candidate dashboard.
- Point-in-time validation helpers.

## Verification

```powershell
python -m pytest
python -m ruff check src tests apps
catalyst-radar init-db
catalyst-radar ingest-csv --securities data/sample/securities.csv --daily-bars data/sample/daily_bars.csv --holdings data/sample/holdings.csv
catalyst-radar scan --as-of 2026-05-08
```

## Current limits

- Data comes from local CSV.
- No SEC/news/text pipeline.
- No local NLP.
- No LLM Decision Cards.
- No broker integration.
- Portfolio holdings ingestion is scaffolded for policy use but not yet wired into scanner state persistence.

## Recommended next phase

Phase 2 should add event and local text intelligence after the deterministic scanner has been reviewed on real daily-bar data.
````

- [ ] **Step 5: Update README with verification section**

Append:

````markdown
## Verification commands

```powershell
python -m pytest
python -m ruff check src tests apps
```

## Phase 1 acceptance

Phase 1 is accepted when:

- sample ingest works from CSV
- scan produces candidate states
- CCC is blocked for liquidity
- dashboard renders current candidates
- all tests pass
- no LLM configuration is required
````

- [ ] **Step 6: Commit review docs**

Run:

```powershell
git add README.md docs/phase-1-review.md
git commit -m "docs: record phase 1 verification"
```

Expected: commit succeeds.

## Self-Review Checklist

- [ ] Spec coverage: Phase 1 covers deterministic scanner, daily bars, securities, feature engine, scoring/policy, portfolio gates, dashboard, tests, and point-in-time validation.
- [ ] Deferred scope is explicit: events, NLP, LLMs, Decision Cards, and broker workflows are excluded from Phase 1.
- [ ] Action states are consistent with v1.1.1 specs.
- [ ] No premium LLM call is required by any task.
- [ ] Every code-changing task includes a failing test, implementation, verification command, and commit command.
- [ ] All exact file paths are specified.
- [ ] The CSV bootstrap connector keeps data-provider selection from blocking deterministic MVP validation.

## Execution Recommendation

Use subagent-driven development if available because these tasks have clean boundaries:

- Task 1-2: foundation and core models
- Task 3-4: storage and ingestion
- Task 5-6: features, scoring, policy, portfolio
- Task 7-9: scanner, dashboard, validation
- Task 10: full verification and review

If executing inline, complete tasks in strict order. Do not start Task 5 before Task 3 passes because feature and scanner tests rely on stable models and storage boundaries.

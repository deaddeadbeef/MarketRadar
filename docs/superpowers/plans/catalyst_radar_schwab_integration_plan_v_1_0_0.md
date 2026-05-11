# Catalyst Radar — Schwab Integration Plan

Version: v1.0.0  
Status: Implementation Plan  
Target: Codex Agent Execution  
Scope: Broker Integration Layer (Schwab)

---

# Objective

Integrate Schwab into Catalyst Radar so the system can:

- read portfolio/account context,
- compute real exposure,
- generate portfolio-aware Decision Cards,
- prepare trade tickets,
- enforce risk controls,
- and optionally support manual order submission later.

The first implementation MUST be read-only by default.

---

# Guiding Rule

```text
Schwab data may inform decisions automatically.
Schwab orders may only be submitted after explicit manual approval.
```

---

# High-Level Architecture

```text
Catalyst Radar Signal Engine
        ↓
Decision Policy Layer
        ↓
Broker Integration Layer
        ↓
Schwab Connector
        ↓
Portfolio Context Service
        ↓
Risk / Exposure Engine
        ↓
Decision Card Builder
        ↓
Manual Trade Review
        ↓
Optional Schwab Order Ticket
```

---

# Phase 1 — Read-Only Schwab Connector

## Goal

Allow Catalyst Radar to:

- connect to Schwab,
- authenticate via OAuth,
- sync balances,
- sync positions,
- sync open orders,
- compute portfolio exposure.

NO order submission.

---

# Services

```text
services/brokers/schwab/
    auth.py
    client.py
    accounts.py
    balances.py
    positions.py
    orders.py
    schemas.py
    token_store.py
    exceptions.py
```

---

# Required API Routes

```text
GET  /api/brokers/schwab/connect
GET  /api/brokers/schwab/callback
GET  /api/brokers/schwab/status
POST /api/brokers/schwab/disconnect

GET  /api/portfolio/snapshot
GET  /api/portfolio/positions
GET  /api/portfolio/balances
GET  /api/portfolio/open-orders
GET  /api/portfolio/exposure
```

---

# Environment Variables

```bash
SCHWAB_CLIENT_ID=
SCHWAB_CLIENT_SECRET=
SCHWAB_REDIRECT_URI=
SCHWAB_ENV=production

SCHWAB_ORDER_SUBMISSION_ENABLED=false

BROKER_TOKEN_ENCRYPTION_KEY=
```

---

# OAuth Flow

```text
1. User visits:
   /api/brokers/schwab/connect

2. Backend redirects to Schwab OAuth authorization page.

3. User approves access.

4. Schwab redirects back:
   /api/brokers/schwab/callback?code=...

5. Backend exchanges code for:
   access_token
   refresh_token

6. Tokens are encrypted and stored.

7. Backend fetches Schwab accounts.

8. Broker connection status becomes CONNECTED.
```

---

# Token Requirements

Must implement:

- encrypted token storage,
- token expiry tracking,
- automatic refresh,
- disconnect flow,
- audit logging.

NEVER store Schwab username/password.

---

# Database Schema

## broker_connections

```sql
CREATE TABLE broker_connections (
    id UUID PRIMARY KEY,
    broker TEXT NOT NULL,
    user_id UUID NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_successful_sync_at TIMESTAMPTZ,
    metadata JSONB
);
```

---

## broker_tokens

```sql
CREATE TABLE broker_tokens (
    id UUID PRIMARY KEY,
    connection_id UUID REFERENCES broker_connections(id),

    access_token_encrypted TEXT NOT NULL,
    refresh_token_encrypted TEXT,

    access_token_expires_at TIMESTAMPTZ NOT NULL,
    refresh_token_expires_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```

---

## broker_accounts

```sql
CREATE TABLE broker_accounts (
    id UUID PRIMARY KEY,

    connection_id UUID REFERENCES broker_connections(id),

    broker TEXT NOT NULL,
    broker_account_id TEXT NOT NULL,

    account_hash TEXT NOT NULL,
    account_type TEXT,

    display_name TEXT,

    is_active BOOLEAN DEFAULT true,

    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,

    UNIQUE (broker, broker_account_id)
);
```

---

## portfolio_snapshots

```sql
CREATE TABLE portfolio_snapshots (
    id UUID PRIMARY KEY,

    account_id UUID REFERENCES broker_accounts(id),

    as_of TIMESTAMPTZ NOT NULL,

    cash NUMERIC,
    buying_power NUMERIC,
    liquidation_value NUMERIC,
    equity NUMERIC,

    raw_payload JSONB
);
```

---

## portfolio_positions

```sql
CREATE TABLE portfolio_positions (
    id UUID PRIMARY KEY,

    account_id UUID REFERENCES broker_accounts(id),

    as_of TIMESTAMPTZ NOT NULL,

    ticker TEXT NOT NULL,

    quantity NUMERIC NOT NULL,

    average_price NUMERIC,
    market_value NUMERIC,
    unrealized_pnl NUMERIC,

    raw_payload JSONB
);
```

---

## broker_orders

```sql
CREATE TABLE broker_orders (
    id UUID PRIMARY KEY,

    account_id UUID REFERENCES broker_accounts(id),

    broker_order_id TEXT,

    ticker TEXT,
    side TEXT,

    order_type TEXT,
    quantity NUMERIC,
    limit_price NUMERIC,

    status TEXT,

    submitted_at TIMESTAMPTZ,

    raw_payload JSONB
);
```

---

## broker_audit_log

```sql
CREATE TABLE broker_audit_log (
    id UUID PRIMARY KEY,

    broker TEXT NOT NULL,
    event_type TEXT NOT NULL,

    user_id UUID,
    account_id UUID,

    ticker TEXT,

    payload JSONB,

    created_at TIMESTAMPTZ NOT NULL
);
```

---

# Broker Client Abstraction

Catalyst Radar must NOT call Schwab directly from business logic.

Use a broker interface.

```python
class BrokerClient(Protocol):

    def get_accounts(self):
        ...

    def get_balances(self, account_id):
        ...

    def get_positions(self, account_id):
        ...

    def get_open_orders(self, account_id):
        ...

    def preview_order(self, account_id, order):
        ...

    def submit_order(self, account_id, order):
        ...
```

Schwab implementation:

```python
class SchwabClient(BrokerClient):
    ...
```

---

# Portfolio Context Service

## Services

```text
services/portfolio/context.py
services/portfolio/exposure.py
services/portfolio/correlation.py
services/portfolio/risk_budget.py
```

---

# Exposure Calculations

```python
single_name_exposure_pct =
    position_market_value / portfolio_equity

sector_exposure_pct =
    sector_market_value / portfolio_equity

theme_exposure_pct =
    theme_market_value / portfolio_equity

correlated_basket_exposure_pct =
    correlated_cluster_value / portfolio_equity
```

---

# Default Exposure Limits

```text
Single-name exposure:
    5%

Theme exposure:
    15%

Sector exposure:
    25%

Correlated basket exposure:
    20%

Risk per trade:
    0.50%

Maximum risk per trade:
    1.00%
```

---

# Decision Card Integration

Every actionable Decision Card must include:

```json
{
  "portfolio_context": {
    "broker_connected": true,

    "portfolio_equity": 250000,

    "cash": 50000,

    "buying_power": 100000,

    "existing_position": {
      "ticker": "GLW",
      "quantity": 100,
      "market_value": 9500,
      "exposure_pct": 3.8
    },

    "exposure_before": {
      "single_name_pct": 3.8,
      "sector_pct": 18.2,
      "theme_pct": 11.5,
      "correlated_basket_pct": 13.2
    },

    "exposure_after_proposed_trade": {
      "single_name_pct": 5.0,
      "sector_pct": 19.4,
      "theme_pct": 13.1,
      "correlated_basket_pct": 15.0
    }
  }
}
```

---

# Trade Ticket Preview

## Route

```text
POST /api/orders/preview
```

---

## Request

```json
{
  "ticker": "GLW",
  "side": "buy",

  "setup_type": "SectorRotationLaggard",

  "entry_price": 95.00,
  "invalidation_price": 90.00,

  "risk_per_trade_pct": 0.50,

  "account_id": "uuid"
}
```

---

## Response

```json
{
  "action": "preview_only",

  "ticker": "GLW",
  "side": "buy",

  "entry_price": 95.00,
  "invalidation_price": 90.00,

  "portfolio_equity": 250000,

  "risk_budget_dollars": 1250,

  "stop_distance": 5.00,

  "max_shares_by_risk": 250,
  "max_shares_by_exposure": 131,

  "proposed_shares": 131,

  "estimated_position_value": 12445,

  "hard_blocks": [],

  "warnings": [
    "Position capped by single-name exposure limit."
  ],

  "requires_manual_approval": true
}
```

---

# Position Sizing Formula

```python
risk_budget_dollars =
    portfolio_equity * risk_per_trade_pct / 100

stop_distance =
    abs(entry_price - invalidation_price)

shares_by_risk =
    floor(risk_budget_dollars / stop_distance)

max_position_value =
    portfolio_equity * max_single_name_exposure_pct / 100

shares_by_exposure =
    floor(max_position_value / entry_price)

proposed_shares =
    min(shares_by_risk, shares_by_exposure)
```

---

# Manual Order Submission

## Disabled by Default

```bash
SCHWAB_ORDER_SUBMISSION_ENABLED=false
```

---

# Submission Requirements

Order submission ONLY allowed when:

```text
- manual confirmation is present
- decision card is eligible
- no hard blocks exist
- broker data is fresh
- invalidation exists
- exposure limits pass
- env flag enabled
```

---

# Order Submission Guard

```python
def can_submit_order(
    decision_card,
    order_preview,
    user_confirmation
):

    if not settings.SCHWAB_ORDER_SUBMISSION_ENABLED:
        return False

    if not user_confirmation.explicitly_confirmed:
        return False

    if decision_card.action_state != "EligibleForManualBuyReview":
        return False

    if decision_card.hard_blocks:
        return False

    if order_preview.proposed_shares <= 0:
        return False

    if order_preview.exposure_after.exceeds_any_cap:
        return False

    if decision_card.invalidation_price is None:
        return False

    return True
```

---

# Dashboard Additions

Add:

```text
Broker connection status
Portfolio snapshot
Positions table
Exposure dashboard
Decision Card portfolio impact
Order preview panel
Audit log
Kill switch status
```

---

# Safety Requirements

## Hard Rules

```text
NO auto-trading in v1.0.0.

NO order submission without explicit approval.

NO order submission with stale broker data.

NO order submission with hard blocks.

NO order submission without invalidation.

NO order submission when exposure limits exceeded.
```

---

# Testing Plan

## Unit Tests

```text
OAuth flow
Token encryption
Token refresh
Position parsing
Exposure calculations
Sizing formulas
Order preview validation
Submission guard logic
```

---

## Integration Tests

```text
OAuth callback
Portfolio sync
Balances sync
Position sync
Decision Card portfolio integration
Order preview generation
Submission blocking
```

---

## Safety Tests

```text
Submission blocked when env flag false
Submission blocked without confirmation
Submission blocked with hard block
Submission blocked without invalidation
Submission blocked on stale data
Submission blocked above exposure caps
```

---

# Implementation Order

```text
1. Add broker domain models
2. Add DB migrations
3. Add encrypted token store
4. Add OAuth connect/callback
5. Add Schwab client
6. Add account sync
7. Add balances sync
8. Add positions sync
9. Add portfolio snapshot service
10. Add exposure engine
11. Add Decision Card portfolio context
12. Add order preview service
13. Add preview API
14. Add audit logging
15. Add dashboard panels
16. Add submission API (disabled)
17. Add kill switch
18. Add tests
19. Add runbook docs
```

---

# Acceptance Criteria

## Read-Only Integration Complete

```text
User can connect Schwab.
Tokens encrypted.
Accounts sync.
Balances sync.
Positions sync.
Exposure calculations correct.
Decision Cards portfolio-aware.
NO order submission possible.
```

---

## Preview System Complete

```text
Order preview works.
Sizing correct.
Hard blocks enforced.
Warnings displayed.
Audit log written.
NO real order submitted.
```

---

## Manual Trading Complete

```text
Submission disabled by default.
Submission requires confirmation.
All submissions logged.
Kill switch works.
Exposure gates enforced.
```


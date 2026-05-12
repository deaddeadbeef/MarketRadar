# Schwab Interactive Dashboard Runbook

Market Radar treats Schwab as a broker data source and interactive decision
surface. Schwab data can inform portfolio context, exposure, sizing previews,
market-context refreshes, and manual review actions. The app does not submit
Schwab orders in this implementation.

## Local Configuration

Set these in `.env.local` or the process environment:

```dotenv
SCHWAB_CLIENT_ID=
SCHWAB_CLIENT_SECRET=
SCHWAB_REDIRECT_URI=https://127.0.0.1:8443/api/brokers/schwab/callback
SCHWAB_ENV=production
SCHWAB_BASE_URL=https://api.schwabapi.com
SCHWAB_AUTH_BASE_URL=https://api.schwabapi.com/v1/oauth
SCHWAB_ORDER_SUBMISSION_ENABLED=false
BROKER_TOKEN_ENCRYPTION_KEY=
```

`BROKER_TOKEN_ENCRYPTION_KEY` must be a local secret. It is used to encrypt
OAuth tokens before they are stored in the application database. Do not commit
real values to git.

## OAuth Flow

1. Start the API server against the target database.
2. Visit `/api/brokers/schwab/connect`.
3. Complete the Schwab consent flow.
4. Schwab redirects to `/api/brokers/schwab/callback`.
5. Market Radar exchanges the code, encrypts tokens, and stores the connection.

If credentials are missing, the connect and callback routes return a clear
`503` explaining which settings are absent.

## Read-Only Sync

After OAuth succeeds, trigger:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/api/brokers/schwab/sync
```

The sync pulls account metadata, balances, positions, and open orders. It stores
broker-specific rows and also writes holdings snapshots so existing portfolio
risk logic can consume synced positions.

## Market Context Sync

To refresh Schwab quote, price-history, and option-chain context for one or more
tickers:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/api/brokers/schwab/market-sync `
  -ContentType application/json `
  -Body '{"tickers":["GLW"],"include_history":true,"include_options":true}'
```

Stored market snapshots are available at:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/market/context
```

## Dashboard

The Streamlit dashboard includes a **Broker** tab with:

- connection status,
- Schwab connect, sync, market refresh, and disconnect controls,
- latest portfolio equity/cash/buying power,
- stale-data status,
- exposure summary,
- market snapshots,
- opportunity action capture,
- price/volume/options trigger creation and evaluation,
- blocked order-ticket previews,
- positions,
- balances,
- open orders.

For local HTTPS OAuth callbacks, the dashboard infers the API origin from
`SCHWAB_REDIRECT_URI`. If the API server is not running, dashboard action
buttons fail visibly without changing stored data.

## Interactive Workflows

Opportunity actions:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/api/opportunities/actions `
  -ContentType application/json `
  -Body '{"ticker":"GLW","action":"watch","thesis":"early volume expansion"}'
```

Triggers:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/api/market/triggers `
  -ContentType application/json `
  -Body '{"ticker":"GLW","trigger_type":"price_above","operator":"gte","threshold":95}'

Invoke-RestMethod -Method Post http://127.0.0.1:8000/api/market/triggers/evaluate `
  -ContentType application/json `
  -Body '{"tickers":["GLW"]}'
```

Blocked order tickets:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/api/orders/tickets `
  -ContentType application/json `
  -Body '{"ticker":"GLW","side":"buy","entry_price":95,"invalidation_price":90}'
```

## Safety Rules

- Schwab username/password are never stored.
- Tokens are encrypted before persistence.
- API responses do not expose raw access or refresh tokens.
- Order preview is manual-review-only and never submits an order.
- Saved order tickets are blocked previews with `submission_allowed=false`.
- There is no order submit/place endpoint.
- `SCHWAB_ORDER_SUBMISSION_ENABLED=true` does not enable submission in this
  read-only integration; it only appears in preview metadata.
- Broker data older than 24 hours is marked stale in portfolio context.

## Useful Checks

```powershell
python -m pytest tests\unit\test_broker_tokens.py tests\integration\test_schwab_broker_sync.py tests\integration\test_broker_interactive_workflows.py tests\integration\test_broker_api_routes.py tests\integration\test_security_boundaries.py -q
python -m ruff check src apps tests
```

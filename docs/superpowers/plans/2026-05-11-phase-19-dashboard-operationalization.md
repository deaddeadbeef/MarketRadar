# Phase 19 - Dashboard Operationalization

## Goal

Finish the adjusted Market Radar goal by turning the Streamlit experience into one deployable command-center dashboard and adding a reproducible data path that proves the current product can be reviewed from a fresh database.

## Scope

- Remove the old `apps/dashboard/pages/*.py` modules so Streamlit no longer exposes a multipage app.
- Keep `apps/dashboard/Home.py` as the single dashboard entry point with all layers in tabs.
- Add a `seed-dashboard-demo` CLI command that creates a candidate, alert, IPO/S-1 row, validation row, cost row, and ops evidence for local review.
- Keep live SEC IPO/S-1 ingestion gated by `CATALYST_SEC_ENABLE_LIVE=1` and `CATALYST_SEC_USER_AGENT`.
- Pass dashboard runtime settings through Docker and production Compose: database URL, `PYTHONPATH`, SEC enable flag, SEC user agent, and SEC base URL.
- Polish dashboard defaults and details: default ticker, selectable dataframes for detail, status badges, and tabular payload display instead of raw JSON.
- Commit the feature branch and fast-forward `main` after verification.

## Files

- `apps/dashboard/Home.py`
- `src/catalyst_radar/cli.py`
- `src/catalyst_radar/dashboard/demo_seed.py`
- `apps/dashboard/pages/*.py`
- `infra/docker/Dockerfile`
- `docker-compose.yml`
- `infra/docker/docker-compose.prod.yml`
- `README.md`
- `tests/integration/test_dashboard_entrypoint.py`
- `tests/integration/test_dashboard_demo_seed_cli.py`
- `tests/integration/test_security_boundaries.py`

## Verification

Run:

```powershell
python -m pytest
python -m ruff check src tests apps
git diff --check
```

Smoke:

```powershell
$env:PYTHONPATH="<worktree>\src;<worktree>"
$env:CATALYST_DATABASE_URL="sqlite:///<worktree>/tmp/phase19-smoke.db"
python -m catalyst_radar.cli seed-dashboard-demo
python -m streamlit run apps/dashboard/Home.py --server.headless true --server.port 8501
```

Then inspect the dashboard in the browser and confirm the command center has no old page entries.

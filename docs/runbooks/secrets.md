# Secrets Runbook

## Local Development

`.env.local` is allowed only for local development and must stay outside production
runtime paths. It is gitignored and must not be copied into images, releases, or
shared support bundles.

Use local environment files only for developer-owned credentials and test
settings. Rotate any credential that appears in shell history, logs, screenshots,
or issue attachments.

## Production Secrets

Production secrets must come from the operator-controlled runtime environment,
not from committed files. Supported patterns are:

- Process manager environment injection.
- Host secret store or VM secret injection.
- Docker or orchestrator secret injection.
- `docker compose --env-file` with a file outside git and controlled by the
  operator.

Required keys:

- `CATALYST_DATABASE_URL`
- `CATALYST_POLYGON_API_KEY` when Polygon ingestion is enabled
- `OPENAI_API_KEY` only when real LLM review is explicitly enabled

Optional security controls:

- `CATALYST_API_AUTH_MODE=header` for API role checks behind a trusted proxy or
  private network.
- `CATALYST_DASHBOARD_AUTH_MODE=header` plus `CATALYST_DASHBOARD_ROLE` for
  Streamlit pilot access checks.

## Rotation

1. Update the authoritative secret source.
2. Restart the API, dashboard, worker, and any scheduler process that reads the
   secret.
3. Confirm `/api/ops/health` reports healthy provider, database, and job state.
4. Run `catalyst-radar llm-budget-status` if LLM credentials changed.
5. Inspect `audit_events` for expected role, feedback, decision, and model-call
   audit rows after the restart.

## Incident Response

1. Stop affected jobs or disable the impacted provider.
2. Run the redaction test suite before collecting logs.
3. Inspect `audit_events` for user decisions, model calls, and hard-block
   bypasses around the incident window.
4. Rotate any credential that may have been exposed.
5. Record the follow-up action and keep the system in decision-support-only mode
   until the incident is closed.

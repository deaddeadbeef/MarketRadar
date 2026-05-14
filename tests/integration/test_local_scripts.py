from pathlib import Path


def test_restart_local_script_restarts_only_market_radar_processes() -> None:
    script = Path("scripts/restart-local.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "apps.api.main:app" in text
    assert "apps/dashboard/Home.py" in text
    assert "apps\\.api\\.main|apps/dashboard/Home\\.py" in text
    assert ".state\\processes" in text
    assert "data\\local\\schwab-localhost-key.pem" in text
    assert "data\\local\\schwab-localhost-cert.pem" in text
    assert "PYTHONPATH" in text
    assert "-Environment" not in text
    assert "curl.exe" in text
    assert "--insecure" in text
    assert "--fail" in text
    assert "ServerCertificateValidationCallback" not in text
    assert "SkipCertificateCheck" not in text


def test_readme_mentions_restart_script_for_local_dashboard() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "scripts/prepare-live-env.ps1" in readme
    assert "scripts/restart-local.ps1" in readme
    assert "scripts/check-live-activation.ps1" in readme
    assert "scripts/run-first-live-smoke.ps1" in readme
    assert "scripts/run-worker-once.ps1" in readme
    assert "scripts/market-radar-status.ps1" in readme
    assert "scripts/assert-investable-readiness.ps1" in readme
    assert "-Execute" in readme
    assert "CATALYST_DAILY_MARKET_PROVIDER=polygon" in readme
    assert "CATALYST_DAILY_PROVIDER=polygon" in readme
    assert "CATALYST_DAILY_MARKET_PROVIDER` controls scheduled daily bar ingest" in readme
    assert "CATALYST_DAILY_PROVIDER` override keeps manual/default radar runs aligned" in readme
    assert "CATALYST_DAILY_EVENT_PROVIDER=sec" in readme
    assert "CATALYST_SEC_ENABLE_LIVE=1" in readme
    assert "/api/radar/runs/call-plan" in readme
    assert "CATALYST_MARKET_PROVIDER=polygon" not in readme


def test_prepare_live_env_script_writes_only_safe_defaults() -> None:
    script = Path("scripts/prepare-live-env.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "External calls made by this script: 0" in text
    assert "SCHWAB_ORDER_SUBMISSION_ENABLED" in text
    assert "false" in text
    assert "CATALYST_RUN_LLM" in text
    assert "CATALYST_LLM_DRY_RUN" in text
    assert "CATALYST_DRY_RUN_ALERTS" in text
    assert "CATALYST_DAILY_MARKET_PROVIDER" in text
    assert "CATALYST_DAILY_PROVIDER" in text
    assert "CATALYST_POLYGON_TICKERS_MAX_PAGES" in text
    assert "CATALYST_SEC_DAILY_MAX_TICKERS" in text
    assert "function Test-EnvKey" in text
    assert 'Set-EnvLine -InputLines $lines -Key $key -Value ""' in text
    assert "CATALYST_POLYGON_API_KEY=placeholder" not in text
    assert "OPENAI_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text


def test_run_first_live_smoke_requires_explicit_execute_for_provider_calls() -> None:
    script = Path("scripts/run-first-live-smoke.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "[switch]$Execute" in text
    assert "/api/radar/live-activation" in text
    assert "/api/radar/runs/call-plan" in text
    assert "/api/radar/universe/seed" in text
    assert "/api/radar/runs" in text
    assert "/api/radar/readiness" in text
    assert "MaxRadarExternalCalls" in text
    assert "MaxUniversePages -gt 1" in text
    assert "Plan only: no provider calls were made." in text
    assert "External calls made: 0" in text
    assert "Re-run with -Execute" in text
    assert "Schwab and OpenAI are not called" in text


def test_run_worker_once_requires_explicit_execute_for_worker_cycle() -> None:
    script = Path("scripts/run-worker-once.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "[switch]$Execute" in text
    assert "/api/radar/live-activation" in text
    assert "/api/radar/runs/call-plan" in text
    assert "Plan only: no provider calls were made and the worker was not started." in text
    assert "CATALYST_WORKER_INTERVAL_SECONDS" in text
    assert "CATALYST_RUN_LLM" in text
    assert "false" in text
    assert "python -m apps.worker.main" in text
    assert "External calls made: 0" in text
    assert "SCHWAB_CLIENT_SECRET=" not in text
    assert "OPENAI_API_KEY=" not in text
    assert "CATALYST_POLYGON_API_KEY=" not in text


def test_market_radar_status_script_is_zero_external_call_sitrep() -> None:
    script = Path("scripts/market-radar-status.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "/api/health" in text
    assert "/api/radar/readiness" in text
    assert "/api/radar/runs/latest" in text
    assert "/api/radar/live-activation" in text
    assert "/api/ops/telemetry?limit=" in text
    assert "audit_rows=" in text
    assert "raw_skips=" not in text
    assert "next safe command" in text
    assert "scripts\\prepare-live-env.ps1" in text
    assert "scripts\\check-live-activation.ps1" in text
    assert "attention=" in text
    assert "guarded=" in text
    assert "External calls made: 0" in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
    assert "OPENAI_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text


def test_assert_investable_readiness_script_fails_closed_without_external_calls() -> None:
    script = Path("scripts/assert-investable-readiness.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "/api/radar/readiness" in text
    assert "/api/radar/live-activation" in text
    assert "/api/radar/runs/call-plan" in text
    assert "/api/ops/telemetry?limit={0}" in text
    assert "[int]$TelemetryLimit = 8" in text
    assert "telemetry_limit" in text
    assert "[string]::IsNullOrWhiteSpace($Body)" in text
    assert "safe_to_make_investment_decision" in text
    assert "Live activation is" in text
    assert "Call plan is blocked" in text
    assert "Telemetry has attention events" in text
    assert "External calls made: 0" in text
    assert "exit 1" in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
    assert "OPENAI_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text


def test_check_live_activation_script_is_zero_external_call_status_check() -> None:
    script = Path("scripts/check-live-activation.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "/api/radar/live-activation" in text
    assert "curl.exe" in text
    assert "--insecure" in text
    assert "--fail" in text
    assert "External calls made by this check: 0" in text
    assert "Zero-call verification commands:" in text
    assert "/api/radar/readiness" in text
    assert "/api/radar/runs/latest" in text
    assert "/api/ops/telemetry?limit=8" in text
    assert "/api/radar/runs/call-plan" in text
    assert "These commands read local API state only" in text
    assert "missing_env" in text
    assert "operator_steps" in text
    assert "OPENAI_API_KEY=" not in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text

import json
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
    assert ".venv\\Scripts\\python.exe" in text
    assert "$PythonExe = if (Test-Path $VenvPython)" in text
    assert "Start-Process -FilePath $PythonExe" in text
    assert "-Environment" not in text
    assert "curl.exe" in text
    assert "--insecure" in text
    assert "--fail" in text
    assert "ServerCertificateValidationCallback" not in text
    assert "SkipCertificateCheck" not in text


def test_readme_mentions_restart_script_for_local_dashboard() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "scripts/prepare-live-env.ps1" in readme
    assert "scripts/open-live-env.ps1" in readme
    assert "scripts/restart-local.ps1" in readme
    assert "scripts/check-live-activation.ps1" in readme
    assert "scripts/run-first-live-smoke.ps1" in readme
    assert "scripts/run-worker-once.ps1" in readme
    assert "scripts/market-radar-status.ps1" in readme
    assert "scripts/market-radar-status.ps1 -Quick" in readme
    assert "catalyst-radar dashboard-tui" in readme
    assert "catalyst-radar dashboard-snapshot --json" in readme
    assert "docs/dashboard-feature-inventory.md" in readme
    assert "catalyst-radar market-bars template" in readme
    assert "catalyst-radar market-bars import" in readme
    assert "scripts/export-telemetry.ps1" in readme
    assert "scripts/export-operator-evidence.ps1" in readme
    assert "scripts/export-pr-ledger.ps1" in readme
    assert "docs\\changes\\pr-ledger.json" in readme
    assert "data\\ops\\bundles\\pr-ledger-current.json" in readme
    assert "scripts/assert-investable-readiness.ps1" in readme
    assert "/api/ops/telemetry/raw" in readme
    assert "-Execute" in readme
    assert "CATALYST_DAILY_MARKET_PROVIDER=csv" in readme
    assert "CATALYST_DAILY_PROVIDER=csv" in readme
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
    assert "[switch]$Quiet" in text
    assert "[switch]$NoNextSteps" in text
    assert "if (-not $Quiet)" in text
    assert "if (-not $NoNextSteps)" in text
    assert "CATALYST_RUN_LLM" in text
    assert "CATALYST_LLM_DRY_RUN" in text
    assert "CATALYST_DRY_RUN_ALERTS" in text
    assert "CATALYST_DAILY_MARKET_PROVIDER" in text
    assert "CATALYST_DAILY_PROVIDER" in text
    assert "CATALYST_POLYGON_TICKERS_MAX_PAGES" in text
    assert "CATALYST_SEC_DAILY_MAX_TICKERS" in text
    assert "Paste the Polygon API key from your Polygon dashboard." not in text
    assert "SEC-compliant contact string" in text
    assert "MarketRadar/0.1 your-email@example.com" in text
    assert "function Test-EnvKey" in text
    assert 'Set-EnvLine -InputLines $lines -Key $key -Value ""' in text
    assert "CATALYST_POLYGON_API_KEY=placeholder" not in text
    assert "OPENAI_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text


def test_open_live_env_script_prefers_vscode_without_external_calls() -> None:
    script = Path("scripts/open-live-env.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "scripts\\prepare-live-env.ps1" in text
    assert "-Quiet" in text
    assert "Get-Command code.cmd" in text
    assert "Get-Command code" in text
    assert "Start-Process -FilePath $codeCommand.Source" in text
    assert "--reuse-window" in text
    assert "notepad.exe" in text
    assert "VS Code" in text
    assert "External calls made by this script: 0" in text
    assert "Safe live defaults are prepared; order submission remains disabled." in text
    assert "Polygon is optional" in text
    assert "CATALYST_SEC_USER_AGENT" in text
    assert "After filling manual values:" in text
    assert "scripts\\restart-local.ps1" in text
    assert "scripts\\check-live-activation.ps1" in text
    assert "scripts\\run-first-live-smoke.ps1" in text
    assert "scripts\\run-first-live-smoke.ps1 -Execute" in text
    assert "OPENAI_API_KEY=" not in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
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
    assert "/api/radar/runs/latest" in text
    assert "/api/radar/readiness" in text
    assert "MaxRadarExternalCalls" in text
    assert "MaxUniversePages -gt 1" in text
    assert "Plan only: no provider calls were made." in text
    assert "External calls made: 0" in text
    assert "Re-run with -Execute" in text
    assert "Schwab and OpenAI are not called" in text
    assert "Radar call plan is blocked" in text
    assert "Run scripts\\open-live-env.ps1" in text
    assert "Only needed if CATALYST_DAILY_MARKET_PROVIDER=polygon." in text
    assert "SEC-compliant contact string" in text
    assert "MarketRadar/0.1 your-email@example.com" in text
    assert "exit 2" in text
    assert "exit 3" in text


def test_run_worker_once_requires_explicit_execute_for_worker_cycle() -> None:
    script = Path("scripts/run-worker-once.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "[switch]$Execute" in text
    assert "/api/radar/live-activation" in text
    assert "/api/radar/runs/call-plan" in text
    assert "Missing live activation values:" in text
    assert "Plan only: no provider calls were made and the worker was not started." in text
    assert "CATALYST_WORKER_INTERVAL_SECONDS" in text
    assert "CATALYST_RUN_LLM" in text
    assert "false" in text
    assert "python -m apps.worker.main" in text
    assert "Worker call plan is blocked" in text
    assert "Run scripts\\open-live-env.ps1" in text
    assert "Only needed if CATALYST_DAILY_MARKET_PROVIDER=polygon." in text
    assert "SEC-compliant contact string" in text
    assert "MarketRadar/0.1 your-email@example.com" in text
    assert "exit 2" in text
    assert "exit 3" in text
    assert "External calls made: 0" in text
    assert "SCHWAB_CLIENT_SECRET=" not in text
    assert "OPENAI_API_KEY=" not in text
    assert "CATALYST_POLYGON_API_KEY=" not in text


def test_market_radar_status_script_is_zero_external_call_sitrep() -> None:
    script = Path("scripts/market-radar-status.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "[switch]$Quick" in text
    assert "/api/health" in text
    assert "/api/radar/readiness" in text
    assert '"repair-plan"' in text
    assert "market_bar_repair_plan" in text
    assert "stock_market_bar_repair_plan" in text
    assert "Market Radar quick status" in text
    assert "Global readiness:" in text
    assert "Full-market priced-in gate:" in text
    assert "Stock priced-in gate:" in text
    assert "stock_like={2}/{3}; missing={4}" in text
    assert "core_order=market_bars,catalyst_events,local_text" in text
    assert "first_gap=" in text
    assert "catalyst-radar priced-in-answer" in text
    assert "catalyst-radar priced-in-answer --stocks-only" in text
    assert "Full-market next:" in text
    assert "Stock-market next:" in text
    assert "Fast market-bar repair:" in text
    assert "Format-FieldCountSummary" in text
    assert "Get-ManualTemplateNextAction" in text
    assert "local bar history:" in text
    assert "missing without local history:" in text
    assert "missing security types:" in text
    assert "incremental complete-row import:" in text
    assert "template schema:" in text
    assert "strict next action:" in text
    assert "stock strict next action:" in text
    assert "stock template schema:" in text
    assert "stock local template fill progress:" in text
    assert "stock provider option: status={0}; health={1}; external_calls={2}; command={3}" in text
    assert "stock provider health warning:" in text
    assert "provider option: status={0}; health={1}; external_calls={2}; command={3}" in text
    assert "provider health warning:" in text
    assert "local template fill progress:" in text
    assert "/api/radar/priced-in/audit?stocks_only=true&limit=1" in text
    assert "[int]$TimeoutSeconds = 15" in text
    assert "-TimeoutSeconds 90" in text
    assert "/api/radar/runs/latest" in text
    assert "/api/radar/live-activation" in text
    assert "/api/radar/runs/call-plan" in text
    assert "/api/brokers/schwab/status" in text
    assert "/api/ops/health" in text
    assert "/api/ops/telemetry?limit=" in text
    assert "/api/ops/telemetry/coverage" in text
    assert "operator_next_step" in text
    assert "readiness_checklist" in text
    assert "market_radar_usefulness" in text
    assert "priced_in_stock_audit" in text
    assert "discovery_snapshot" in text
    assert "Operator next:" in text
    assert "Usefulness:" in text
    assert "safe_decision=" in text
    assert "ready_layers=" in text
    assert "useful means:" in text
    assert "Stock priced-in scan:" in text
    assert "decision_ready=" in text
    assert "stock answer lens:" in text
    assert "stock coverage-first gap:" in text
    assert "stock coverage command:" in text
    assert "/api/radar/priced-in/source-batches?source={0}&stocks_only=true&batch_limit=1" in text
    assert "priced_in_stock_coverage_batch_plan" in text
    assert "batches" in text
    assert "external_calls_required" in text
    assert "stock coverage batch plan:" in text
    assert "eligible_rows" in text
    assert "blocked_rows" in text
    assert "next_calls=" in text
    assert "stock coverage missing bars" in text
    assert "stock coverage missing CIK" in text
    assert "sample_blocked_tickers" in text
    assert "stock bar template:" in text
    assert "stock CIK template" in text
    assert "manual_template_command" in text
    assert "stock bar validate:" in text
    assert "stock CIK validate" in text
    assert "manual_validate_command" in text
    assert "stock bar import:" in text
    assert "stock CIK import" in text
    assert "manual_fix_command" in text
    assert "stock bar refresh:" in text
    assert "stock CIK refresh" in text
    assert "fix_command" in text
    assert "stock decision-context gap:" in text
    assert "stock point-in-time template:" in text
    assert "point_in_time_template_command" in text
    assert "stock point-in-time validate:" in text
    assert "point_in_time_validate_command" in text
    assert "stock point-in-time import:" in text
    assert "point_in_time_import_command" in text
    assert "Market freshness:" in text
    assert "latest_bar=" in text
    assert "run_as_of=" in text
    assert "Market as-of coverage:" in text
    assert "with_as_of_bar=" in text
    assert "missing as-of tickers:" in text
    assert "Market coverage:" in text
    assert "active=" in text
    assert "with_bars=" in text
    assert "with_latest_bar=" in text
    assert "latest-bar coverage:" in text
    assert "missing latest-bar tickers:" in text
    assert "Generate the missing-bar template" in text
    assert "catalyst-radar market-bars template" in text
    assert "--missing-only" in text
    assert "Stock-like market bars:" in text
    assert "stock-like template command:" in text
    assert "stock-like provider option:" in text
    assert "stock-like provider boundary:" in text
    assert "provider_call_command" in text
    assert "manual-stock-bars-" in text
    assert "--stocks-only" in text
    assert "catalyst-radar market-bars import" in text
    assert "--expected-as-of" in text
    assert "catalyst_radar.cli" in text
    assert "manual_market_bar_preview" in text
    assert "local template preview:" in text
    assert "local template blank fields:" in text
    assert "local template invalid examples:" in text
    assert "Portfolio context:" in text
    assert "Broker:" in text
    assert "access_token_active=" in text
    assert "refresh_token_available=" in text
    assert "Call plan:" in text
    assert "will_call_external=" in text
    assert "max_external_calls=" in text
    assert "audit_rows=" in text
    assert "raw_skips=" not in text
    assert "next safe command" in text
    assert "scripts\\open-live-env.ps1" in text
    assert "scripts\\check-live-activation.ps1" in text
    assert "scripts\\run-first-live-smoke.ps1" in text
    assert "scripts\\run-first-live-smoke.ps1 -Execute" in text
    assert "attention=" in text
    assert "guarded=" in text
    assert "Telemetry coverage:" in text
    assert "missing_required=" in text
    assert "External calls made: 0" in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
    assert "OPENAI_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text


def test_refresh_csv_market_data_script_wraps_local_ingest_without_provider_calls() -> None:
    script = Path("scripts/refresh-csv-market-data.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "[switch]$Execute" in text
    assert "data/sample/securities.csv" in text
    assert "data/sample/daily_bars.csv" in text
    assert "ExpectedAsOf" in text
    assert "TemplateOut" in text
    assert "CSV market data template" in text
    assert "Fill open, high, low, close, volume, and vwap before importing." in text
    assert "data\\local\\manual-bars-" in Path("scripts/market-radar-status.ps1").read_text(
        encoding="utf-8"
    )
    assert "Assert-DailyBarRows" in text
    assert "Daily bars CSV validation failed:" in text
    assert "missing required numeric field" in text
    assert "Fix the rows above, then preview again before importing." in text
    assert "Coverage check:" in text
    assert "Refusing to import incomplete bars" in text
    assert "fill the generated ticker rows" in text
    assert "Import-Csv" in text
    assert "latest_bar=" in text
    assert "Freshness check:" in text
    assert "Refusing to import stale bars" in text
    assert "Plan only: no database writes were made." in text
    assert "py @cliArgs" in text
    assert "catalyst_radar.cli" in text
    assert "ingest-csv" in text
    assert "External calls made: 0" in text
    assert "scripts\\market-radar-status.ps1" in text
    assert "scripts\\run-first-live-smoke.ps1" in text
    assert "OPENAI_API_KEY=" not in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
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
    assert "Only needed if CATALYST_DAILY_MARKET_PROVIDER=polygon." in text
    assert "SEC-compliant contact string" in text
    assert "MarketRadar/0.1 your-email@example.com" in text
    assert "OPENAI_API_KEY=" not in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text


def test_run_full_market_scan_script_is_plan_first_and_execute_gated() -> None:
    script = Path("scripts/run-full-market-scan.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "priced-in-preflight" in text
    assert "--json" in text
    assert "preflight.target_as_of" in text
    assert "preflight.target_as_of_source" in text
    assert "provider.latest_daily_bar_date" in text
    assert "Plan only: no provider calls or database writes were made." in text
    assert "Invoke-DailyRunAcceptCompletedScan" in text
    assert "feature_scan" in text
    assert "-Execute" in text
    assert "-AllowPartial" in text
    assert "-UseUniverse" in text
    assert "-RefreshTickers" in text
    assert "Scan scope: all active securities with available bars" in text
    assert "Active universe:" in text
    assert "Ticker seed: {0}; configured_pages={1}; estimated_pages={2}; selected_pages={3}" in text
    assert (
        "Execute provider calls: ticker_pages={0}; grouped_daily=1; "
        "total={1}; call_plan_max={2}"
    ) in text
    assert "plannedProviderCalls -gt $maxExternalCalls" in text
    assert "exceeds call_plan_max" in text
    assert "if ($shouldSeedTickers)" in text
    assert "active universe is already seeded" in text
    assert "Skipping Polygon ticker seed" in text
    assert "CATALYST_POLYGON_TICKERS_MAX_PAGES" in text
    assert "CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS" in text
    assert "CATALYST_DAILY_MARKET_PROVIDER" in text
    assert "CATALYST_UNIVERSE_NAME" in text
    assert "ingest-polygon\", \"tickers\"" in text
    assert "ingest-polygon\", \"grouped-daily\"" in text
    assert "build-universe" in text
    assert "--available-at" in text
    assert "--provider\", \"polygon\"" in text
    assert "--universe" in text
    assert "Scan as-of: {0}; source={1}" in text
    assert (
        '"run-daily", "--as-of", $resolvedAsOf, "--available-at", $availableAt, '
        '"--provider", "polygon", "--json"'
    ) in text
    assert "run-daily" in text
    assert "scan\", \"--as-of\"" not in text
    assert "priced-in-queue" in text
    assert "External calls made: 0" in text
    assert "OPENAI_API_KEY=" not in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text


def test_export_telemetry_script_writes_zero_call_raw_snapshot() -> None:
    script = Path("scripts/export-telemetry.ps1")
    text = script.read_text(encoding="utf-8")
    gitignore = Path(".gitignore").read_text(encoding="utf-8")

    assert script.is_file()
    assert "/api/ops/telemetry/raw?limit=$resolvedLimit" in text
    assert "data\\ops\\telemetry" in text
    assert "telemetry-export-$stamp.json" in text
    assert "ConvertFrom-Json" in text
    assert "External calls made: 0" in text
    assert "curl.exe" in text
    assert "--insecure" in text
    assert "--fail" in text
    assert "data/ops/telemetry/" in gitignore
    assert "OPENAI_API_KEY=" not in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text


def test_export_operator_evidence_script_writes_zero_call_bundle() -> None:
    script = Path("scripts/export-operator-evidence.ps1")
    text = script.read_text(encoding="utf-8")
    gitignore = Path(".gitignore").read_text(encoding="utf-8")

    assert script.is_file()
    assert "operator-evidence-bundle-v1" in text
    assert "/api/health" in text
    assert "/api/radar/readiness" in text
    assert "/api/radar/runs/latest" in text
    assert "/api/radar/live-activation" in text
    assert "/api/radar/runs/call-plan" in text
    assert "/api/ops/telemetry?limit={0}" in text
    assert "/api/ops/telemetry/coverage" in text
    assert "/api/ops/telemetry/raw?limit={0}" in text
    assert "/api/brokers/schwab/status" in text
    assert "docs\\changes\\pr-ledger.json" in text
    assert "data\\ops\\bundles\\pr-ledger-current.json" in text
    assert "change_ledger" in text
    assert "change_ledger_source" in text
    assert "change_ledger_path" in text
    assert "operator_next_step" in text
    assert "operator_next_action" in text
    assert "telemetry_coverage" in text
    assert "telemetry_coverage_missing_required" in text
    assert "tracked_merged_prs" in text
    assert "latest_tracked_pr" in text
    assert "schwab_connection_status" in text
    assert "schwab_access_token_active" in text
    assert "schwab_refresh_token_available" in text
    assert "data\\ops\\bundles" in text
    assert "operator-evidence-$stamp.json" in text
    assert "External calls made: 0" in text
    assert "data/ops/bundles/" in gitignore
    assert "OPENAI_API_KEY=" not in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text


def test_export_pr_ledger_script_tracks_pull_request_changes() -> None:
    script = Path("scripts/export-pr-ledger.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "gh pr list" in text
    assert "--state all" in text
    assert "--json number,title,state,mergedAt,headRefName,baseRefName,url,mergeCommit" in text
    assert "pr-change-ledger-v1" in text
    assert "docs\\changes\\pr-ledger.json" in text
    assert "github_metadata_calls_made = 1" in text
    assert "market_data_broker_llm_calls_made = 0" in text
    assert "Market-data/broker/LLM calls made: 0" in text
    assert "OPENAI_API_KEY=" not in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text


def test_checked_in_pr_ledger_snapshot_is_machine_readable() -> None:
    ledger = Path("docs/changes/pr-ledger.json")
    payload = json.loads(ledger.read_text(encoding="utf-8"))

    assert payload["schema_version"] == "pr-change-ledger-v1"
    assert payload["status"] == "tracked"
    assert payload["market_data_broker_llm_calls_made"] == 0
    assert payload["github_metadata_calls_made"] == 1
    assert payload["total_merged"] >= 1
    assert payload["entries"]
    assert {"number", "title", "merged_at", "merge_commit", "url"}.issubset(
        payload["entries"][-1]
    )

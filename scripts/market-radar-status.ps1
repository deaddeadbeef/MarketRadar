param(
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8443,
    [int]$TelemetryLimit = 3,
    [switch]$Quick,
    [switch]$Json
)

$ErrorActionPreference = "Stop"
$baseUrl = "https://$ApiHost`:$ApiPort"

function Invoke-ApiJson {
    param(
        [string]$Path,
        [string]$Method = "GET",
        [string]$Body = $null,
        [int]$TimeoutSeconds = 15
    )

    $curlArgs = @(
        "--insecure",
        "--silent",
        "--show-error",
        "--fail",
        "--max-time",
        ([string]$TimeoutSeconds),
        "--request",
        $Method,
        "$baseUrl$Path"
    )
    if ([string]::IsNullOrWhiteSpace($Body) -eq $false) {
        $curlArgs += @("--header", "Content-Type: application/json", "--data-raw", $Body)
    }
    $response = & curl.exe @curlArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Could not read local API status from $baseUrl$Path. Start services with scripts\restart-local.ps1."
    }
    try {
        return $response | ConvertFrom-Json
    }
    catch {
        throw "Local API returned invalid JSON for $Path."
    }
}

function Format-FieldCountSummary {
    param([object]$Value)

    if ($null -eq $Value) {
        return ""
    }
    $pairs = @()
    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($key in $Value.Keys) {
            $count = $Value[$key]
            if ($null -ne $count) {
                $pairs += ("{0}={1}" -f $key, $count)
            }
        }
    }
    else {
        foreach ($property in @($Value.PSObject.Properties)) {
            if ($null -ne $property.Value) {
                $pairs += ("{0}={1}" -f $property.Name, $property.Value)
            }
        }
    }
    return ($pairs -join ", ")
}

$health = Invoke-ApiJson -Path "/api/health"
$readiness = Invoke-ApiJson -Path "/api/radar/readiness"
$pythonExe = ".\.venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $pythonExe)) {
    $pythonExe = "py"
}
$marketBarRepairPlan = $null
$manualMarketBarPreview = $null
if ($readiness.radar_run.as_of) {
    $runAsOf = [string]$readiness.radar_run.as_of
    try {
        $repairArgs = @(
            "-m",
            "catalyst_radar.cli",
            "market-bars",
            "repair-plan",
            "--expected-as-of",
            $runAsOf,
            "--json"
        )
        $repairResponse = & $pythonExe @repairArgs 2>$null
        if ([string]::IsNullOrWhiteSpace(($repairResponse -join "`n"))) {
            throw "market-bar repair plan returned no JSON"
        }
        $marketBarRepairPlan = ($repairResponse -join "`n") | ConvertFrom-Json
    }
    catch {
        $marketBarRepairPlan = [ordered]@{
            status = "error"
            expected_as_of = $runAsOf
            stocks_only = $false
            detail = $_.Exception.Message
            external_calls_made = 0
        }
    }
    $manualStockBarsPath = "data\local\manual-stock-bars-$runAsOf.csv"
    $manualAllBarsPath = "data\local\manual-bars-$runAsOf.csv"
    $manualBarsPath = $manualAllBarsPath
    $manualBarsStocksOnly = $false
    if (
        -not (Test-Path -LiteralPath $manualAllBarsPath) -and
        (Test-Path -LiteralPath $manualStockBarsPath)
    ) {
        $manualBarsPath = $manualStockBarsPath
        $manualBarsStocksOnly = $true
    }
    if (Test-Path -LiteralPath $manualBarsPath) {
        try {
            $previewArgs = @(
                "-m",
                "catalyst_radar.cli",
                "market-bars",
                "import",
                "--daily-bars",
                $manualBarsPath,
                "--expected-as-of",
                $runAsOf,
                "--json"
            )
            if ($manualBarsStocksOnly) {
                $previewArgs += "--stocks-only"
            }
            $previewResponse = & $pythonExe @previewArgs 2>$null
            if ([string]::IsNullOrWhiteSpace(($previewResponse -join "`n"))) {
                throw "manual market-bar preview returned no JSON"
            }
            $manualMarketBarPreview = ($previewResponse -join "`n") | ConvertFrom-Json
        }
        catch {
            $manualMarketBarPreview = [ordered]@{
                status = "error"
                daily_bars_path = $manualBarsPath
                detail = $_.Exception.Message
                external_calls_made = 0
            }
        }
    }
}

if ($Quick) {
    $payload = [ordered]@{
        health = $health
        readiness = $readiness
        market_bar_repair_plan = $marketBarRepairPlan
        external_calls_made = 0
    }
    if ($null -ne $manualMarketBarPreview) {
        $payload["manual_market_bar_preview"] = $manualMarketBarPreview
    }
    if ($Json) {
        $payload | ConvertTo-Json -Depth 12
        return
    }
    $build = $health.build
    Write-Output "Market Radar quick status"
    Write-Output ("API: {0}; build={1}; version={2}" -f $health.status, $build.commit, $build.version)
    Write-Output (
        "Global readiness: {0}; investable={1}; next={2}" -f
        $readiness.status,
        $readiness.safe_to_make_investment_decision,
        $readiness.next_action
    )
    if ($null -ne $marketBarRepairPlan) {
        Write-Output (
            "Full-market next: bars={0}/{1}; missing={2}; command={3}" -f
            $marketBarRepairPlan.existing_as_of_bar_count,
            $marketBarRepairPlan.active_security_count,
            $marketBarRepairPlan.missing_as_of_bar_count,
            $marketBarRepairPlan.manual_template_command
        )
        Write-Output (
            "Fast market-bar repair: status={0}; scope={1}; active={2}; existing={3}; missing={4}; external_calls={5}" -f
            $marketBarRepairPlan.status,
            $marketBarRepairPlan.coverage_scope,
            $marketBarRepairPlan.active_security_count,
            $marketBarRepairPlan.existing_as_of_bar_count,
            $marketBarRepairPlan.missing_as_of_bar_count,
            $marketBarRepairPlan.external_calls_made
        )
        Write-Output ("- manual template: {0}" -f $marketBarRepairPlan.manual_template_command)
        Write-Output ("- preview import: {0}" -f $marketBarRepairPlan.manual_import_preview_command)
        if ($marketBarRepairPlan.provider_fill_command) {
            Write-Output (
                "- provider option: status={0}; external_calls={1}; command={2}" -f
                $marketBarRepairPlan.provider_fill_status,
                $marketBarRepairPlan.provider_fill_external_call_count,
                $marketBarRepairPlan.provider_fill_command
            )
            Write-Output ("- provider boundary: {0}" -f $marketBarRepairPlan.approval_boundary)
        }
    }
    if ($null -ne $manualMarketBarPreview) {
        Write-Output (
            "- local template preview: status={0}; rows={1}; invalid_rows={2}; blank_required={3}; missing_after_import={4}; external_calls={5}" -f
            $manualMarketBarPreview.status,
            $(if ($null -ne $manualMarketBarPreview.row_count) { $manualMarketBarPreview.row_count } else { "n/a" }),
            $(if ($null -ne $manualMarketBarPreview.invalid_row_count) { $manualMarketBarPreview.invalid_row_count } else { "n/a" }),
            $(if ($null -ne $manualMarketBarPreview.blank_required_count) { $manualMarketBarPreview.blank_required_count } else { "n/a" }),
            $(if ($null -ne $manualMarketBarPreview.missing_expected_count) { $manualMarketBarPreview.missing_expected_count } else { "n/a" }),
            $(if ($null -ne $manualMarketBarPreview.external_calls_made) { $manualMarketBarPreview.external_calls_made } else { 0 })
        )
        if ($manualMarketBarPreview.next_action) {
            Write-Output ("- local template next: {0}" -f $manualMarketBarPreview.next_action)
        }
        $blankFieldSummary = Format-FieldCountSummary -Value $manualMarketBarPreview.blank_required_field_counts
        if (-not [string]::IsNullOrWhiteSpace($blankFieldSummary)) {
            Write-Output ("- local template blank fields: {0}" -f $blankFieldSummary)
        }
        $invalidExamples = @(
            $manualMarketBarPreview.invalid_examples |
                Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }
        )
        if ($invalidExamples.Count -gt 0) {
            Write-Output ("- local template invalid examples: {0}" -f (($invalidExamples | Select-Object -First 3) -join " | "))
        }
    }
    Write-Output "External calls made: 0"
    return
}

$pricedInStockAudit = Invoke-ApiJson -Path "/api/radar/priced-in/audit?stocks_only=true&limit=1" -TimeoutSeconds 90
$latestRun = Invoke-ApiJson -Path "/api/radar/runs/latest"
$activation = Invoke-ApiJson -Path "/api/radar/live-activation"
$callPlan = Invoke-ApiJson -Method "POST" -Path "/api/radar/runs/call-plan" -Body "{}"
$brokerStatus = Invoke-ApiJson -Path "/api/brokers/schwab/status"
$opsHealth = Invoke-ApiJson -Path "/api/ops/health"
$telemetry = Invoke-ApiJson -Path ("/api/ops/telemetry?limit={0}" -f [Math]::Max(1, $TelemetryLimit))
$telemetryCoverage = Invoke-ApiJson -Path "/api/ops/telemetry/coverage"

$payload = [ordered]@{
    health = $health
    readiness = $readiness
    market_bar_repair_plan = $marketBarRepairPlan
    priced_in_stock_audit = $pricedInStockAudit
    latest_run = $latestRun
    live_activation = $activation
    call_plan = $callPlan
    broker_status = $brokerStatus
    ops_health = $opsHealth
    telemetry = $telemetry
    telemetry_coverage = $telemetryCoverage
    external_calls_made = 0
}

$portfolioContext = $null
foreach ($row in @($readiness.readiness_checklist)) {
    if ($row.area -eq "Portfolio context") {
        $portfolioContext = $row
        break
    }
}

$usefulness = $readiness.market_radar_usefulness
$stockScope = $pricedInStockAudit.scope
$stockAnswer = $pricedInStockAudit.answer_shortlist
$stockEvidence = $pricedInStockAudit.evidence_plan
$stockBarScope = $pricedInStockAudit.market_bars.repair.stock_scope
$stockBarProviderFill = $pricedInStockAudit.market_bars.repair.provider_fill_plan
$stockCoverageStep = $null
$stockCoverageBatchPlan = $null
if ($null -ne $stockEvidence) {
    foreach ($step in @($stockEvidence.steps)) {
        if ($step.status -ne "ready") {
            $stockCoverageStep = $step
            break
        }
    }
}
if ($null -ne $stockCoverageStep -and $stockCoverageStep.area) {
    $coverageSource = [System.Uri]::EscapeDataString([string]$stockCoverageStep.area)
    $stockCoverageBatchPlan = Invoke-ApiJson `
        -Path ("/api/radar/priced-in/source-batches?source={0}&stocks_only=true&batch_limit=1" -f $coverageSource) `
        -TimeoutSeconds 90
    $payload["priced_in_stock_coverage_batch_plan"] = $stockCoverageBatchPlan
}
$stockRecommendedSource = $pricedInStockAudit.recommended_source_gap
$stockRecommendedRepair = $null
if ($null -ne $stockRecommendedSource) {
    foreach ($sourceRow in @($pricedInStockAudit.sources)) {
        if ($sourceRow.source -eq $stockRecommendedSource.source) {
            $stockRecommendedRepair = $sourceRow.repair
            break
        }
    }
}
$discovery = $readiness.discovery_snapshot
$freshness = $null
if ($null -ne $discovery) {
    $freshness = $discovery.freshness
}
$staleBarBlocker = $null
foreach ($blocker in @($discovery.blockers)) {
    if ($blocker.code -eq "stale_daily_bars") {
        $staleBarBlocker = $blocker
        break
    }
}
$databaseHealth = $opsHealth.database
if ($null -ne $manualMarketBarPreview) {
    $payload["manual_market_bar_preview"] = $manualMarketBarPreview
}

if ($Json) {
    $payload | ConvertTo-Json -Depth 12
    return
}

$build = $health.build
Write-Output "Market Radar local status"
Write-Output ("API: {0}; build={1}; version={2}" -f $health.status, $build.commit, $build.version)
Write-Output (
    "Readiness: {0}; investable={1}; next={2}" -f
    $readiness.status,
    $readiness.safe_to_make_investment_decision,
    $readiness.next_action
)
if ($null -ne $readiness.operator_next_step) {
    Write-Output (
        "Operator next: {0}; priority={1}; area={2}" -f
        $readiness.operator_next_step.action,
        $readiness.operator_next_step.priority,
        $readiness.operator_next_step.area
    )
}
if ($null -ne $usefulness) {
    Write-Output (
        "Usefulness: {0}; safe_decision={1}; ready_layers={2}/{3}; blocked={4}; research={5}" -f
        $usefulness.status,
        $usefulness.safe_to_make_investment_decision,
        $usefulness.ready_layers,
        $usefulness.total_layers,
        $usefulness.blocked_layers,
        $usefulness.research_layers
    )
    Write-Output (
        "- useful means: research triage requires a complete required run path and labeled sources; manual investment review requires fresh market bars, live catalysts, a Decision Card, and no blockers."
    )
}
if ($null -ne $stockScope) {
    Write-Output (
        "Stock priced-in scan: status={0}; ranked={1}; scanned={2}; decision_ready={3}; external_calls={4}" -f
        $pricedInStockAudit.status,
        $stockScope.ranked_rows,
        $stockScope.scanned_rows,
        $(if ($null -ne $stockAnswer) { $stockAnswer.decision_ready_rows } else { "n/a" }),
        $pricedInStockAudit.external_calls_made
    )
    if ($null -ne $stockAnswer) {
        Write-Output (
            "- stock answer lens: {0}; actionable={1}; boundary={2}" -f
            $stockAnswer.status,
            $stockAnswer.actionable_mismatch_rows,
            $stockAnswer.investment_decision_boundary
        )
    }
    if ($null -ne $stockCoverageStep) {
        Write-Output (
            "- stock coverage-first gap: {0}; {1} Next: {2}" -f
            $stockCoverageStep.area,
            $stockCoverageStep.why,
            $stockCoverageStep.action
        )
    }
    if ($null -ne $stockCoverageStep -and $stockCoverageStep.command) {
        Write-Output ("- stock coverage command: {0}" -f $stockCoverageStep.command)
    }
    $coverageDiagnostic = $null
    $coverageFirstBatch = $null
    if ($null -ne $stockCoverageBatchPlan) {
        $coverageDiagnostic = $stockCoverageBatchPlan.diagnostic
        $coverageFirstBatch = $stockCoverageBatchPlan.first_batch
        if ($null -eq $coverageFirstBatch -and $stockCoverageBatchPlan.batches) {
            $coverageFirstBatch = @($stockCoverageBatchPlan.batches)[0]
        }
    }
    if ($null -ne $coverageDiagnostic -and $null -ne $coverageDiagnostic.eligible_rows) {
        $coverageNextCalls = "0"
        if ($null -ne $coverageFirstBatch -and $null -ne $coverageFirstBatch.external_calls_required) {
            $coverageNextCalls = $coverageFirstBatch.external_calls_required
        }
        Write-Output (
            "- stock coverage SEC plan: eligible={0}; blocked={1}; next_calls={2}; blocked_reason={3}" -f
            $coverageDiagnostic.eligible_rows,
            $coverageDiagnostic.blocked_rows,
            $coverageNextCalls,
            $(if ($coverageDiagnostic.blocked_reason) { $coverageDiagnostic.blocked_reason } else { "none" })
        )
    }
    if ($null -ne $coverageDiagnostic -and $coverageDiagnostic.sample_blocked_tickers) {
        Write-Output (
            "- stock coverage missing CIK: {0}" -f
            ((@($coverageDiagnostic.sample_blocked_tickers) | Select-Object -First 12) -join ", ")
        )
    }
    if ($null -ne $coverageDiagnostic -and $coverageDiagnostic.manual_template_command) {
        Write-Output ("- stock CIK template: {0}" -f $coverageDiagnostic.manual_template_command)
    }
    if ($null -ne $coverageDiagnostic -and $coverageDiagnostic.manual_validate_command) {
        Write-Output ("- stock CIK validate: {0}" -f $coverageDiagnostic.manual_validate_command)
    }
    if ($null -ne $coverageDiagnostic -and $coverageDiagnostic.manual_fix_command) {
        Write-Output ("- stock CIK import: {0}" -f $coverageDiagnostic.manual_fix_command)
    }
    if ($null -ne $coverageDiagnostic -and $coverageDiagnostic.fix_command) {
        Write-Output ("- stock CIK refresh: {0}" -f $coverageDiagnostic.fix_command)
    }
    if ($null -ne $stockRecommendedSource) {
        Write-Output (
            "- stock decision-context gap: {0}; gaps={1}; {2}" -f
            $stockRecommendedSource.source,
            $stockRecommendedSource.gap_count,
            $stockRecommendedSource.next_action
        )
        if ($null -ne $stockRecommendedRepair -and $stockRecommendedRepair.point_in_time_template_command) {
            Write-Output ("- stock point-in-time template: {0}" -f $stockRecommendedRepair.point_in_time_template_command)
        }
        if ($null -ne $stockRecommendedRepair -and $stockRecommendedRepair.point_in_time_validate_command) {
            Write-Output ("- stock point-in-time validate: {0}" -f $stockRecommendedRepair.point_in_time_validate_command)
        }
        if ($null -ne $stockRecommendedRepair -and $stockRecommendedRepair.point_in_time_import_command) {
            Write-Output ("- stock point-in-time import: {0}" -f $stockRecommendedRepair.point_in_time_import_command)
        }
    }
}
if ($null -ne $freshness) {
    Write-Output (
        "Market freshness: stale={0}; latest_bar={1}; run_as_of={2}" -f
        $(if ($null -ne $freshness.latest_bars_older_than_as_of) { $freshness.latest_bars_older_than_as_of } else { "n/a" }),
        $(if ($freshness.latest_daily_bar_date) { $freshness.latest_daily_bar_date } else { "n/a" }),
        $(if ($readiness.radar_run.as_of) { $readiness.radar_run.as_of } else { "n/a" })
    )
    if ($null -ne $freshness.active_security_with_as_of_bar_count -and $null -ne $freshness.active_security_count) {
        Write-Output (
            "Market as-of coverage: active={0}; with_as_of_bar={1}; missing={2}" -f
            $freshness.active_security_count,
            $freshness.active_security_with_as_of_bar_count,
            $(if ($null -ne $freshness.missing_as_of_daily_bar_count) { $freshness.missing_as_of_daily_bar_count } else { "n/a" })
        )
        $missingAsOf = @($freshness.missing_as_of_daily_bar_tickers)
        if ($missingAsOf.Count -gt 0) {
            Write-Output ("- missing as-of tickers: {0}" -f (($missingAsOf | Select-Object -First 12) -join ", "))
        }
    }
    if ($staleBarBlocker.next_action) {
        Write-Output ("- market freshness: {0}" -f $staleBarBlocker.next_action)
    }
    if ($readiness.radar_run.as_of) {
        if ($null -ne $stockBarScope) {
            Write-Output (
                "Stock-like market bars: active={0}; with_as_of_bar={1}; missing={2}; non_stock_missing={3}" -f
                $stockBarScope.stock_like_active,
                $stockBarScope.stock_like_with_as_of_bar,
                $stockBarScope.stock_like_missing_as_of_bar,
                $stockBarScope.non_stock_missing_as_of_bar
            )
            Write-Output (
                "- stock-like template command: catalyst-radar market-bars template --expected-as-of {0} --out data\local\manual-stock-bars-{0}.csv --missing-only --stocks-only" -f
                $readiness.radar_run.as_of
            )
            Write-Output (
                "- stock-like preview command: catalyst-radar market-bars import --daily-bars data\local\manual-stock-bars-{0}.csv --expected-as-of {0} --stocks-only" -f
                $readiness.radar_run.as_of
            )
            if ($null -ne $stockBarProviderFill -and $stockBarProviderFill.provider_call_command) {
                Write-Output (
                    "- stock-like provider option: status={0}; external_calls={1}; command={2}" -f
                    $stockBarProviderFill.status,
                    $(if ($null -ne $stockBarProviderFill.execute_external_call_count) { $stockBarProviderFill.execute_external_call_count } else { 0 }),
                    $stockBarProviderFill.provider_call_command
                )
                Write-Output "- stock-like provider boundary: run only after explicit approval; grouped daily writes local bars, then rerun audit."
            }
        }
        Write-Output (
            "- template command: catalyst-radar market-bars template --expected-as-of {0} --out data\local\manual-bars-{0}.csv --missing-only" -f
            $readiness.radar_run.as_of
        )
        Write-Output (
            "- refresh command: catalyst-radar market-bars import --daily-bars <fresh-bars.csv> --expected-as-of {0} --execute" -f
            $readiness.radar_run.as_of
        )
        if ($null -ne $manualMarketBarPreview) {
            Write-Output (
                "- local template preview: status={0}; rows={1}; invalid_rows={2}; blank_required={3}; missing_after_import={4}; external_calls={5}" -f
                $manualMarketBarPreview.status,
                $(if ($null -ne $manualMarketBarPreview.row_count) { $manualMarketBarPreview.row_count } else { "n/a" }),
                $(if ($null -ne $manualMarketBarPreview.invalid_row_count) { $manualMarketBarPreview.invalid_row_count } else { "n/a" }),
                $(if ($null -ne $manualMarketBarPreview.blank_required_count) { $manualMarketBarPreview.blank_required_count } else { "n/a" }),
                $(if ($null -ne $manualMarketBarPreview.missing_expected_count) { $manualMarketBarPreview.missing_expected_count } else { "n/a" }),
                $(if ($null -ne $manualMarketBarPreview.external_calls_made) { $manualMarketBarPreview.external_calls_made } else { 0 })
            )
            $invalidExamples = @(
                $manualMarketBarPreview.invalid_examples |
                    Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }
            )
            if ($invalidExamples.Count -gt 0) {
                Write-Output ("- local template invalid examples: {0}" -f (($invalidExamples | Select-Object -First 3) -join " | "))
            }
            $blankFieldSummary = Format-FieldCountSummary -Value $manualMarketBarPreview.blank_required_field_counts
            if (-not [string]::IsNullOrWhiteSpace($blankFieldSummary)) {
                Write-Output ("- local template blank fields: {0}" -f $blankFieldSummary)
            }
            if ($manualMarketBarPreview.detail) {
                Write-Output ("- local template preview error: {0}" -f $manualMarketBarPreview.detail)
            }
            if ($manualMarketBarPreview.next_action) {
                Write-Output ("- local template next: {0}" -f $manualMarketBarPreview.next_action)
            }
        }
    }
}
if ($null -ne $databaseHealth) {
    $activeSecurityCount = $(if ($null -ne $databaseHealth.active_security_count) { $databaseHealth.active_security_count } else { "n/a" })
    $withAnyBarsCount = $(if ($null -ne $databaseHealth.active_security_with_daily_bar_count) { $databaseHealth.active_security_with_daily_bar_count } else { "n/a" })
    $withLatestBarsCount = $databaseHealth.active_security_with_latest_daily_bar_count
    if ($null -eq $withLatestBarsCount) {
        $withLatestBarsCount = $withAnyBarsCount
    }
    Write-Output (
        "Market coverage: active={0}; with_bars={1}; with_latest_bar={2}; latest_bar={3}" -f
        $activeSecurityCount,
        $withAnyBarsCount,
        $withLatestBarsCount,
        $(if ($databaseHealth.latest_daily_bar_date) { $databaseHealth.latest_daily_bar_date } else { "n/a" })
    )
    if (
        $null -ne $databaseHealth.active_security_count -and
        $null -ne $databaseHealth.active_security_with_daily_bar_count -and
        [int]$databaseHealth.active_security_with_daily_bar_count -lt [int]$databaseHealth.active_security_count
    ) {
        Write-Output "- market coverage: Generate the missing-bar template and fill only missing ticker rows before import."
    }
    if (
        $null -ne $databaseHealth.active_security_count -and
        $null -ne $databaseHealth.active_security_with_latest_daily_bar_count -and
        [int]$databaseHealth.active_security_with_latest_daily_bar_count -lt [int]$databaseHealth.active_security_count
    ) {
        Write-Output "- latest-bar coverage: Fill only missing ticker rows for the latest/as-of date before treating bars as fresh."
        $missingLatest = @($databaseHealth.missing_latest_daily_bar_tickers)
        if ($missingLatest.Count -gt 0) {
            Write-Output ("- missing latest-bar tickers: {0}" -f (($missingLatest | Select-Object -First 12) -join ", "))
        }
    }
}
if ($null -ne $portfolioContext) {
    Write-Output (
        "Portfolio context: {0}; {1}" -f
        $portfolioContext.status,
        $portfolioContext.finding
    )
    if ($portfolioContext.next_action) {
        Write-Output ("- portfolio: {0}" -f $portfolioContext.next_action)
    }
}
Write-Output (
    "Broker: connected={0}; connection_status={1}; access_token_active={2}; refresh_token_available={3}; order_submission_available={4}" -f
    $brokerStatus.connected,
    $(if ($null -ne $brokerStatus.connection_status) { $brokerStatus.connection_status } else { $brokerStatus.status }),
    $(if ($null -ne $brokerStatus.access_token_active) { $brokerStatus.access_token_active } else { "n/a" }),
    $(if ($null -ne $brokerStatus.refresh_token_available) { $brokerStatus.refresh_token_available } else { "n/a" }),
    $brokerStatus.order_submission_available
)
Write-Output (
    "Latest run: {0}; required={1}/{2}; action_needed={3}; optional_gates={4}; audit_rows={5}" -f
    $latestRun.status,
    $latestRun.required_completed_count,
    $latestRun.required_step_count,
    $latestRun.action_needed_count,
    $latestRun.optional_expected_gate_count,
    $latestRun.status_counts.skipped
)
Write-Output (
    "Live activation: {0}; missing={1}" -f
    $activation.status,
    @($activation.missing_env).Count
)
Write-Output (
    "Call plan: {0}; will_call_external={1}; max_external_calls={2}; next={3}" -f
    $callPlan.status,
    $callPlan.will_call_external_providers,
    $callPlan.max_external_call_count,
    $callPlan.next_action
)
if (@($activation.missing_env).Count -gt 0) {
    foreach ($item in @($activation.missing_env)) {
        Write-Output ("- missing: {0}" -f $item)
    }
    Write-Output "- next safe command: powershell -ExecutionPolicy Bypass -File scripts\open-live-env.ps1"
    Write-Output "- after editing .env.local: powershell -ExecutionPolicy Bypass -File scripts\restart-local.ps1"
    Write-Output "- verify again: powershell -ExecutionPolicy Bypass -File scripts\check-live-activation.ps1"
    Write-Output "- plan-only smoke: powershell -ExecutionPolicy Bypass -File scripts\run-first-live-smoke.ps1"
    Write-Output "- capped live smoke only if plan matches intent: powershell -ExecutionPolicy Bypass -File scripts\run-first-live-smoke.ps1 -Execute"
}
Write-Output (
    "Telemetry: {0}; events={1}; attention={2}; guarded={3}; latest={4}" -f
    $telemetry.status,
    $telemetry.event_count,
    $(if ($null -ne $telemetry.attention_count) { $telemetry.attention_count } else { "n/a" }),
    $(if ($null -ne $telemetry.guarded_count) { $telemetry.guarded_count } else { "n/a" }),
    $telemetry.latest_event_at
)
if ($telemetry.headline -or $telemetry.next_action) {
    Write-Output ("- telemetry: {0} Next: {1}" -f $telemetry.headline, $telemetry.next_action)
}
Write-Output (
    "Telemetry coverage: {0}; required_ready={1}/{2}; missing_required={3}; latest={4}" -f
    $telemetryCoverage.status,
    $telemetryCoverage.ready_required_domain_count,
    $telemetryCoverage.required_domain_count,
    $telemetryCoverage.missing_required_count,
    $telemetryCoverage.latest_event_at
)
if ($telemetryCoverage.headline -or $telemetryCoverage.next_action) {
    Write-Output (
        "- telemetry coverage: {0} Next: {1}" -f
        $telemetryCoverage.headline,
        $telemetryCoverage.next_action
    )
}
foreach ($event in @($telemetry.events)) {
    Write-Output (
        "- {0}: {1}; {2}" -f
        $event.occurred_at,
        $event.event,
        $event.status
    )
}
Write-Output "External calls made: 0"

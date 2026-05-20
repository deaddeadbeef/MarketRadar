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

function Get-ManualTemplateNextAction {
    param(
        [object]$Preview,
        [object]$RepairPlan
    )

    if (
        $null -ne $RepairPlan -and
        $null -ne $RepairPlan.operator_step -and
        -not [string]::IsNullOrWhiteSpace([string]$RepairPlan.operator_step.action)
    ) {
        return [string]$RepairPlan.operator_step.action
    }
    if (
        $null -ne $Preview -and
        -not [string]::IsNullOrWhiteSpace([string]$Preview.next_action)
    ) {
        return [string]$Preview.next_action
    }
    return $null
}

function Get-RepairPlanNextCommand {
    param(
        [object]$RepairPlan,
        [string]$Fallback = $null
    )

    if ($null -ne $RepairPlan -and $null -ne $RepairPlan.operator_step) {
        $step = $RepairPlan.operator_step
        if (-not [string]::IsNullOrWhiteSpace([string]$step.command)) {
            return [string]$step.command
        }
        if (-not [string]::IsNullOrWhiteSpace([string]$step.after_manual_command)) {
            return [string]$step.after_manual_command
        }
    }
    if (
        $null -ne $RepairPlan -and
        -not [string]::IsNullOrWhiteSpace([string]$RepairPlan.manual_template_command)
    ) {
        return [string]$RepairPlan.manual_template_command
    }
    return $Fallback
}

function Write-SavedFileCaptureApproval {
    param(
        [string]$Label,
        [object]$Packet
    )

    if ($null -eq $Packet) {
        return
    }
    Write-Output (
        "- {0} saved-file capture approval: status={1}; approval_required={2}; missing={3}; external_calls_without_approval={4}; external_calls_if_approved={5}; db_writes_during_capture={6}; confirm={7}" -f
        $Label,
        $(if ($Packet.status) { $Packet.status } else { "unknown" }),
        $(if ($null -ne $Packet.approval_required) { $Packet.approval_required } else { $false }),
        $(if ($null -ne $Packet.missing_as_of_bar_count) { $Packet.missing_as_of_bar_count } else { "n/a" }),
        $(if ($null -ne $Packet.external_calls_without_approval) { $Packet.external_calls_without_approval } else { 0 }),
        $(if ($null -ne $Packet.external_calls_if_approved) { $Packet.external_calls_if_approved } else { 0 }),
        $(if ($null -ne $Packet.db_writes_during_capture) { $Packet.db_writes_during_capture } else { 0 }),
        $(if ($Packet.tui_confirm_command) { $Packet.tui_confirm_command } else { "n/a" })
    )
    if ($Packet.question) {
        Write-Output ("- {0} saved-file capture question: {1}" -f $Label, $Packet.question)
    }
}

$health = Invoke-ApiJson -Path "/api/health"
$readiness = Invoke-ApiJson -Path "/api/radar/readiness"
$pythonExe = ".\.venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $pythonExe)) {
    $pythonExe = "py"
}
$marketBarRepairPlan = $null
$stockMarketBarRepairPlan = $null
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
    try {
        $stockRepairArgs = @(
            "-m",
            "catalyst_radar.cli",
            "market-bars",
            "repair-plan",
            "--expected-as-of",
            $runAsOf,
            "--stocks-only",
            "--json"
        )
        $stockRepairResponse = & $pythonExe @stockRepairArgs 2>$null
        if ([string]::IsNullOrWhiteSpace(($stockRepairResponse -join "`n"))) {
            throw "stock market-bar repair plan returned no JSON"
        }
        $stockMarketBarRepairPlan = ($stockRepairResponse -join "`n") | ConvertFrom-Json
    }
    catch {
        $stockMarketBarRepairPlan = [ordered]@{
            status = "error"
            expected_as_of = $runAsOf
            stocks_only = $true
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
        stock_market_bar_repair_plan = $stockMarketBarRepairPlan
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
    $coreEvidenceStatus = "unknown"
    $coreFirstGap = "market_bars"
    $coreEvidenceCommand = "catalyst-radar priced-in-answer"
    if (
        $null -ne $marketBarRepairPlan -and
        $null -ne $marketBarRepairPlan.missing_as_of_bar_count -and
        [int]$marketBarRepairPlan.missing_as_of_bar_count -gt 0
    ) {
        $coreEvidenceStatus = "blocked"
        $coreFirstGap = "market_bars"
        $coreEvidenceCommand = Get-RepairPlanNextCommand `
            -RepairPlan $marketBarRepairPlan `
            -Fallback $marketBarRepairPlan.manual_template_command
    }
    elseif ($null -ne $marketBarRepairPlan -and $marketBarRepairPlan.status -eq "ready") {
        $coreEvidenceStatus = "market_bars_ready"
        $coreFirstGap = "catalyst_events_or_local_text"
    }
    Write-Output (
        "Full-market priced-in gate: status={0}; first_gap={1}; core_order=market_bars,catalyst_events,local_text; command={2}; external_calls=0" -f
        $coreEvidenceStatus,
        $coreFirstGap,
        $coreEvidenceCommand
    )
    if ($null -ne $stockMarketBarRepairPlan) {
        $stockEvidenceStatus = "unknown"
        $stockFirstGap = "market_bars"
        $stockEvidenceCommand = "catalyst-radar priced-in-answer --stocks-only"
        if (
            $null -ne $stockMarketBarRepairPlan.missing_as_of_bar_count -and
            [int]$stockMarketBarRepairPlan.missing_as_of_bar_count -gt 0
        ) {
            $stockEvidenceStatus = "blocked"
            $stockFirstGap = "market_bars"
            $stockEvidenceCommand = Get-RepairPlanNextCommand `
                -RepairPlan $stockMarketBarRepairPlan `
                -Fallback $stockMarketBarRepairPlan.manual_template_command
        }
        elseif ($stockMarketBarRepairPlan.status -eq "ready") {
            $stockEvidenceStatus = "market_bars_ready"
            $stockFirstGap = "catalyst_events_or_local_text"
        }
        Write-Output (
            "Stock priced-in gate: status={0}; first_gap={1}; stock_like={2}/{3}; missing={4}; core_order=market_bars,catalyst_events,local_text; command={5}; external_calls=0" -f
            $stockEvidenceStatus,
            $stockFirstGap,
            $stockMarketBarRepairPlan.existing_as_of_bar_count,
            $stockMarketBarRepairPlan.active_security_count,
            $stockMarketBarRepairPlan.missing_as_of_bar_count,
            $stockEvidenceCommand
        )
    }
    if ($null -ne $marketBarRepairPlan) {
        Write-Output (
            "Full-market next: bars={0}/{1}; missing={2}; command={3}" -f
            $marketBarRepairPlan.existing_as_of_bar_count,
            $marketBarRepairPlan.active_security_count,
            $marketBarRepairPlan.missing_as_of_bar_count,
            (Get-RepairPlanNextCommand `
                -RepairPlan $marketBarRepairPlan `
                -Fallback $marketBarRepairPlan.manual_template_command)
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
        $marketTemplateSchema = $marketBarRepairPlan.local_template_schema
        if ($null -ne $marketTemplateSchema) {
            $missingContext = @(
                $marketTemplateSchema.missing_context_columns |
                    Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }
            )
            if ($missingContext.Count -gt 0) {
                Write-Output (
                    "- template schema: status={0}; missing_context={1}; regenerate={2}" -f
                    $marketTemplateSchema.status,
                    ($missingContext -join ","),
                    $marketBarRepairPlan.manual_template_regenerate_command
                )
            }
        }
        Write-Output ("- preview import: {0}" -f $marketBarRepairPlan.manual_import_preview_command)
        if ($marketBarRepairPlan.manual_incremental_import_execute_command) {
            Write-Output ("- incremental complete-row import: {0}" -f $marketBarRepairPlan.manual_incremental_import_execute_command)
        }
        if ($null -ne $marketBarRepairPlan.operator_step) {
            $step = $marketBarRepairPlan.operator_step
            $stepCommand = $(if ($step.command) { $step.command } else { "manual" })
            $afterManual = $(if ($step.after_manual_command) { $step.after_manual_command } else { "n/a" })
            Write-Output (
                "- strict next action: status={0}; manual={1}; external_calls={2}; action={3}; command={4}; after_manual={5}" -f
                $step.status,
                $step.manual_step,
                $(if ($null -ne $step.external_calls_made) { $step.external_calls_made } else { 0 }),
                $step.action,
                $stepCommand,
                $afterManual
            )
        }
        Write-Output (
            "- local bar history: missing_with_history={0}; missing_without_history={1}" -f
            $(if ($null -ne $marketBarRepairPlan.missing_with_local_history_count) { $marketBarRepairPlan.missing_with_local_history_count } else { "n/a" }),
            $(if ($null -ne $marketBarRepairPlan.missing_without_local_history_count) { $marketBarRepairPlan.missing_without_local_history_count } else { "n/a" })
        )
        $noHistory = @(
            $marketBarRepairPlan.missing_without_local_history_sample |
                Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }
        )
        if ($noHistory.Count -gt 0) {
            Write-Output ("- missing without local history: {0}" -f (($noHistory | Select-Object -First 12) -join ", "))
        }
        $missingTypeSummary = Format-FieldCountSummary -Value $marketBarRepairPlan.missing_security_type_counts
        if (-not [string]::IsNullOrWhiteSpace($missingTypeSummary)) {
            Write-Output ("- missing security types: {0}" -f $missingTypeSummary)
        }
        if ($null -ne $marketBarRepairPlan.missing_universe_diagnostic) {
            $diag = $marketBarRepairPlan.missing_universe_diagnostic
            Write-Output (
                "- missing universe diagnostics: active_metadata={0}; acquisition_or_spac_names={1}; no_composite_figi={2}; zero_avg_dollar_volume_20d={3}; zero_market_cap={4}; external_calls={5}" -f
                $(if ($null -ne $diag.active_metadata_rows) { $diag.active_metadata_rows } else { "n/a" }),
                $(if ($null -ne $diag.acquisition_or_spac_name_count) { $diag.acquisition_or_spac_name_count } else { "n/a" }),
                $(if ($null -ne $diag.no_composite_figi_count) { $diag.no_composite_figi_count } else { "n/a" }),
                $(if ($null -ne $diag.zero_avg_dollar_volume_20d_count) { $diag.zero_avg_dollar_volume_20d_count } else { "n/a" }),
                $(if ($null -ne $diag.zero_market_cap_count) { $diag.zero_market_cap_count } else { "n/a" }),
                $(if ($null -ne $diag.external_calls_made) { $diag.external_calls_made } else { 0 })
            )
            if ($diag.operator_note) {
                Write-Output ("- missing universe note: {0}" -f $diag.operator_note)
            }
        }
        if ($marketBarRepairPlan.provider_fill_command) {
            $providerHealth = $marketBarRepairPlan.provider_health
            $providerHealthText = $(if ($null -ne $providerHealth -and $providerHealth.status) { $providerHealth.status } else { "unknown" })
            Write-Output (
                "- provider option: status={0}; health={1}; external_calls={2}; command={3}" -f
                $marketBarRepairPlan.provider_fill_status,
                $providerHealthText,
                $marketBarRepairPlan.provider_fill_external_call_count,
                $marketBarRepairPlan.provider_fill_command
            )
            if ($null -ne $providerHealth -and $providerHealth.reason) {
                Write-Output ("- provider health reason: {0}" -f $providerHealth.reason)
            }
            if ($marketBarRepairPlan.provider_health_warning) {
                Write-Output ("- provider health warning: {0}" -f $marketBarRepairPlan.provider_health_warning)
            }
            Write-Output ("- provider boundary: {0}" -f $marketBarRepairPlan.approval_boundary)
        }
        if ($marketBarRepairPlan.provider_saved_file_import_command) {
            Write-Output (
                "- provider saved-file status: status={0}; exists={1}; path={2}; next={3}" -f
                $(if ($marketBarRepairPlan.provider_saved_file_status) { $marketBarRepairPlan.provider_saved_file_status } else { "unknown" }),
                $(if ($null -ne $marketBarRepairPlan.provider_saved_file_exists) { $marketBarRepairPlan.provider_saved_file_exists } else { $false }),
                $(if ($marketBarRepairPlan.provider_saved_file_path) { $marketBarRepairPlan.provider_saved_file_path } else { "n/a" }),
                $(if ($marketBarRepairPlan.provider_saved_file_next_action) { $marketBarRepairPlan.provider_saved_file_next_action } else { "n/a" })
            )
            if ($marketBarRepairPlan.provider_saved_file_capture_command) {
                Write-Output (
                    "- provider saved-file capture: external_calls={0}; command={1}" -f
                    $(if ($null -ne $marketBarRepairPlan.provider_saved_file_capture_external_call_count) { $marketBarRepairPlan.provider_saved_file_capture_external_call_count } else { 1 }),
                    $marketBarRepairPlan.provider_saved_file_capture_command
                )
            }
            Write-SavedFileCaptureApproval `
                -Label "provider" `
                -Packet $marketBarRepairPlan.provider_saved_file_capture_approval_packet
            if ($marketBarRepairPlan.provider_saved_file_validate_command) {
                Write-Output (
                    "- provider saved-file validate: external_calls={0}; command={1}" -f
                    $(if ($null -ne $marketBarRepairPlan.provider_saved_file_external_call_count) { $marketBarRepairPlan.provider_saved_file_external_call_count } else { 0 }),
                    $marketBarRepairPlan.provider_saved_file_validate_command
                )
            }
            Write-Output (
                "- provider saved-file import: external_calls={0}; command={1}" -f
                $(if ($null -ne $marketBarRepairPlan.provider_saved_file_external_call_count) { $marketBarRepairPlan.provider_saved_file_external_call_count } else { 0 }),
                $marketBarRepairPlan.provider_saved_file_import_command
            )
            if ($marketBarRepairPlan.provider_saved_file_boundary) {
                Write-Output ("- provider saved-file boundary: {0}" -f $marketBarRepairPlan.provider_saved_file_boundary)
            }
        }
    }
    if ($null -ne $stockMarketBarRepairPlan) {
        Write-Output (
            "Stock-market next: bars={0}/{1}; missing={2}; command={3}" -f
            $stockMarketBarRepairPlan.existing_as_of_bar_count,
            $stockMarketBarRepairPlan.active_security_count,
            $stockMarketBarRepairPlan.missing_as_of_bar_count,
            (Get-RepairPlanNextCommand `
                -RepairPlan $stockMarketBarRepairPlan `
                -Fallback $stockMarketBarRepairPlan.manual_template_command)
        )
        if ($null -ne $stockMarketBarRepairPlan.operator_step) {
            $stockStep = $stockMarketBarRepairPlan.operator_step
            $stockStepCommand = $(if ($stockStep.command) { $stockStep.command } else { "manual" })
            $stockAfterManual = $(if ($stockStep.after_manual_command) { $stockStep.after_manual_command } else { "n/a" })
            Write-Output (
                "- stock strict next action: status={0}; manual={1}; external_calls={2}; action={3}; command={4}; after_manual={5}" -f
                $stockStep.status,
                $stockStep.manual_step,
                $(if ($null -ne $stockStep.external_calls_made) { $stockStep.external_calls_made } else { 0 }),
                $stockStep.action,
                $stockStepCommand,
                $stockAfterManual
            )
        }
        $stockTemplateSchema = $stockMarketBarRepairPlan.local_template_schema
        if ($null -ne $stockTemplateSchema) {
            $stockMissingContext = @(
                $stockTemplateSchema.missing_context_columns |
                    Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }
            )
            if ($stockMissingContext.Count -gt 0) {
                Write-Output (
                    "- stock template schema: status={0}; missing_context={1}; regenerate={2}" -f
                    $stockTemplateSchema.status,
                    ($stockMissingContext -join ","),
                    $stockMarketBarRepairPlan.manual_template_regenerate_command
                )
            }
        }
        $stockPreview = $stockMarketBarRepairPlan.local_template_preview
        if ($null -ne $stockPreview) {
            $stockFillProgress = $stockPreview.fill_progress
            if ($null -ne $stockFillProgress) {
                Write-Output (
                    "- stock local template fill progress: complete={0}; partial={1}; empty={2}; filled={3}" -f
                    $(if ($null -ne $stockFillProgress.complete_rows) { $stockFillProgress.complete_rows } else { 0 }),
                    $(if ($null -ne $stockFillProgress.partial_rows) { $stockFillProgress.partial_rows } else { 0 }),
                    $(if ($null -ne $stockFillProgress.empty_rows) { $stockFillProgress.empty_rows } else { 0 }),
                    $(if ($null -ne $stockFillProgress.filled_rows) { $stockFillProgress.filled_rows } else { 0 })
                )
            }
        }
        if ($null -ne $stockMarketBarRepairPlan.missing_universe_diagnostic) {
            $stockDiag = $stockMarketBarRepairPlan.missing_universe_diagnostic
            Write-Output (
                "- stock missing universe diagnostics: active_metadata={0}; acquisition_or_spac_names={1}; no_composite_figi={2}; zero_avg_dollar_volume_20d={3}; zero_market_cap={4}; external_calls={5}" -f
                $(if ($null -ne $stockDiag.active_metadata_rows) { $stockDiag.active_metadata_rows } else { "n/a" }),
                $(if ($null -ne $stockDiag.acquisition_or_spac_name_count) { $stockDiag.acquisition_or_spac_name_count } else { "n/a" }),
                $(if ($null -ne $stockDiag.no_composite_figi_count) { $stockDiag.no_composite_figi_count } else { "n/a" }),
                $(if ($null -ne $stockDiag.zero_avg_dollar_volume_20d_count) { $stockDiag.zero_avg_dollar_volume_20d_count } else { "n/a" }),
                $(if ($null -ne $stockDiag.zero_market_cap_count) { $stockDiag.zero_market_cap_count } else { "n/a" }),
                $(if ($null -ne $stockDiag.external_calls_made) { $stockDiag.external_calls_made } else { 0 })
            )
        }
        if ($stockMarketBarRepairPlan.provider_fill_command) {
            $stockProviderHealth = $stockMarketBarRepairPlan.provider_health
            $stockProviderHealthText = $(if ($null -ne $stockProviderHealth -and $stockProviderHealth.status) { $stockProviderHealth.status } else { "unknown" })
            Write-Output (
                "- stock provider option: status={0}; health={1}; external_calls={2}; command={3}" -f
                $stockMarketBarRepairPlan.provider_fill_status,
                $stockProviderHealthText,
                $stockMarketBarRepairPlan.provider_fill_external_call_count,
                $stockMarketBarRepairPlan.provider_fill_command
            )
            if ($stockMarketBarRepairPlan.provider_health_warning) {
                Write-Output ("- stock provider health warning: {0}" -f $stockMarketBarRepairPlan.provider_health_warning)
            }
        }
        if ($stockMarketBarRepairPlan.provider_saved_file_import_command) {
            Write-Output (
                "- stock provider saved-file status: status={0}; exists={1}; path={2}; next={3}" -f
                $(if ($stockMarketBarRepairPlan.provider_saved_file_status) { $stockMarketBarRepairPlan.provider_saved_file_status } else { "unknown" }),
                $(if ($null -ne $stockMarketBarRepairPlan.provider_saved_file_exists) { $stockMarketBarRepairPlan.provider_saved_file_exists } else { $false }),
                $(if ($stockMarketBarRepairPlan.provider_saved_file_path) { $stockMarketBarRepairPlan.provider_saved_file_path } else { "n/a" }),
                $(if ($stockMarketBarRepairPlan.provider_saved_file_next_action) { $stockMarketBarRepairPlan.provider_saved_file_next_action } else { "n/a" })
            )
            if ($stockMarketBarRepairPlan.provider_saved_file_capture_command) {
                Write-Output (
                    "- stock provider saved-file capture: external_calls={0}; command={1}" -f
                    $(if ($null -ne $stockMarketBarRepairPlan.provider_saved_file_capture_external_call_count) { $stockMarketBarRepairPlan.provider_saved_file_capture_external_call_count } else { 1 }),
                    $stockMarketBarRepairPlan.provider_saved_file_capture_command
                )
            }
            Write-SavedFileCaptureApproval `
                -Label "stock provider" `
                -Packet $stockMarketBarRepairPlan.provider_saved_file_capture_approval_packet
            if ($stockMarketBarRepairPlan.provider_saved_file_validate_command) {
                Write-Output (
                    "- stock provider saved-file validate: external_calls={0}; command={1}" -f
                    $(if ($null -ne $stockMarketBarRepairPlan.provider_saved_file_external_call_count) { $stockMarketBarRepairPlan.provider_saved_file_external_call_count } else { 0 }),
                    $stockMarketBarRepairPlan.provider_saved_file_validate_command
                )
            }
            Write-Output (
                "- stock provider saved-file import: external_calls={0}; command={1}" -f
                $(if ($null -ne $stockMarketBarRepairPlan.provider_saved_file_external_call_count) { $stockMarketBarRepairPlan.provider_saved_file_external_call_count } else { 0 }),
                $stockMarketBarRepairPlan.provider_saved_file_import_command
            )
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
        $manualPreviewRepairPlan = $(if ($manualBarsStocksOnly) { $stockMarketBarRepairPlan } else { $marketBarRepairPlan })
        $manualTemplateNext = Get-ManualTemplateNextAction -Preview $manualMarketBarPreview -RepairPlan $manualPreviewRepairPlan
        if ($manualTemplateNext) {
            Write-Output ("- local template next: {0}" -f $manualTemplateNext)
        }
        $fillProgress = $manualMarketBarPreview.fill_progress
        if ($null -ne $fillProgress) {
            Write-Output (
                "- local template fill progress: complete={0}; partial={1}; empty={2}; filled={3}" -f
                $(if ($null -ne $fillProgress.complete_rows) { $fillProgress.complete_rows } else { 0 }),
                $(if ($null -ne $fillProgress.partial_rows) { $fillProgress.partial_rows } else { 0 }),
                $(if ($null -ne $fillProgress.empty_rows) { $fillProgress.empty_rows } else { 0 }),
                $(if ($null -ne $fillProgress.filled_rows) { $fillProgress.filled_rows } else { 0 })
            )
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
    stock_market_bar_repair_plan = $stockMarketBarRepairPlan
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
    if ($stockRecommendedSource.source -eq "market_bars") {
        $stockCoverageStep = $stockRecommendedSource
    }
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
    $stockBarMissing = 0
    if ($null -ne $stockBarScope -and $null -ne $stockBarScope.stock_like_missing_as_of_bar) {
        $stockBarMissing = [int]$stockBarScope.stock_like_missing_as_of_bar
    }
    $stockCoverageArea = $(if ($stockBarMissing -gt 0) { "market_bars" } elseif ($stockCoverageStep.source) { [string]$stockCoverageStep.source } else { [string]$stockCoverageStep.area })
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
        if ($stockBarMissing -gt 0 -and $null -ne $stockBarScope) {
            $coverageFinding = "stock-like bars {0}/{1}; missing={2}" -f $stockBarScope.stock_like_with_as_of_bar, $stockBarScope.stock_like_active, $stockBarMissing
            $coverageAction = $stockBarScope.next_action
        }
        else {
            $coverageFinding = $(if ($stockCoverageStep.why) { $stockCoverageStep.why } elseif ($null -ne $stockCoverageStep.gap_count) { "gap rows={0}" -f $stockCoverageStep.gap_count } else { "needs attention" })
            $coverageAction = $(if ($stockCoverageStep.next_action) { $stockCoverageStep.next_action } else { $stockCoverageStep.action })
        }
        Write-Output (
            "- stock coverage-first gap: {0}; {1} Next: {2}" -f
            $stockCoverageArea,
            $coverageFinding,
            $coverageAction
        )
    }
    $stockCoverageCommand = $(if ($stockBarMissing -gt 0 -and $null -ne $stockBarScope) { Get-RepairPlanNextCommand -RepairPlan $stockBarScope -Fallback $stockBarScope.manual_template_command } elseif ($null -ne $stockCoverageStep) { $stockCoverageStep.command } else { $null })
    if ($stockCoverageCommand) {
        Write-Output ("- stock coverage command: {0}" -f $stockCoverageCommand)
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
            "- stock coverage batch plan: eligible={0}; blocked={1}; next_calls={2}; blocked_reason={3}" -f
            $coverageDiagnostic.eligible_rows,
            $coverageDiagnostic.blocked_rows,
            $coverageNextCalls,
            $(if ($coverageDiagnostic.blocked_reason) { $coverageDiagnostic.blocked_reason } else { "none" })
        )
    }
    if ($null -ne $coverageDiagnostic -and $coverageDiagnostic.sample_blocked_tickers) {
        $blockedTickers = ((@($coverageDiagnostic.sample_blocked_tickers) | Select-Object -First 12) -join ", ")
        if ($stockCoverageArea -eq "market_bars") {
            Write-Output ("- stock coverage missing bars: {0}" -f $blockedTickers)
        }
        elseif ($stockCoverageArea -eq "catalyst_events") {
            Write-Output ("- stock coverage missing CIK: {0}" -f $blockedTickers)
        }
        else {
            Write-Output ("- stock coverage blocked tickers: {0}" -f $blockedTickers)
        }
    }
    if ($null -ne $coverageDiagnostic -and $coverageDiagnostic.manual_template_command) {
        if ($stockCoverageArea -eq "market_bars") {
            Write-Output ("- stock bar template: {0}" -f $coverageDiagnostic.manual_template_command)
        }
        elseif ($stockCoverageArea -eq "catalyst_events") {
            Write-Output ("- stock CIK template: {0}" -f $coverageDiagnostic.manual_template_command)
        }
        else {
            Write-Output ("- stock coverage template: {0}" -f $coverageDiagnostic.manual_template_command)
        }
    }
    if ($null -ne $coverageDiagnostic -and $coverageDiagnostic.manual_validate_command) {
        if ($stockCoverageArea -eq "market_bars") {
            Write-Output ("- stock bar validate: {0}" -f $coverageDiagnostic.manual_validate_command)
        }
        elseif ($stockCoverageArea -eq "catalyst_events") {
            Write-Output ("- stock CIK validate: {0}" -f $coverageDiagnostic.manual_validate_command)
        }
        else {
            Write-Output ("- stock coverage validate: {0}" -f $coverageDiagnostic.manual_validate_command)
        }
    }
    if ($null -ne $coverageDiagnostic -and $coverageDiagnostic.manual_fix_command) {
        if ($stockCoverageArea -eq "market_bars") {
            Write-Output ("- stock bar import: {0}" -f $coverageDiagnostic.manual_fix_command)
        }
        elseif ($stockCoverageArea -eq "catalyst_events") {
            Write-Output ("- stock CIK import: {0}" -f $coverageDiagnostic.manual_fix_command)
        }
        else {
            Write-Output ("- stock coverage import: {0}" -f $coverageDiagnostic.manual_fix_command)
        }
    }
    if ($null -ne $coverageDiagnostic -and $coverageDiagnostic.fix_command) {
        if ($stockCoverageArea -eq "market_bars") {
            Write-Output ("- stock bar refresh: {0}" -f $coverageDiagnostic.fix_command)
        }
        elseif ($stockCoverageArea -eq "catalyst_events") {
            Write-Output ("- stock CIK refresh: {0}" -f $coverageDiagnostic.fix_command)
        }
        else {
            Write-Output ("- stock coverage refresh: {0}" -f $coverageDiagnostic.fix_command)
        }
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
            $manualPreviewRepairPlan = $(if ($manualBarsStocksOnly) { $stockMarketBarRepairPlan } else { $marketBarRepairPlan })
            $manualTemplateNext = Get-ManualTemplateNextAction -Preview $manualMarketBarPreview -RepairPlan $manualPreviewRepairPlan
            if ($manualTemplateNext) {
                Write-Output ("- local template next: {0}" -f $manualTemplateNext)
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

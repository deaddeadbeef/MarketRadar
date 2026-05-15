param(
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8443,
    [int]$TelemetryLimit = 3,
    [switch]$Json
)

$ErrorActionPreference = "Stop"
$baseUrl = "https://$ApiHost`:$ApiPort"

function Invoke-ApiJson {
    param(
        [string]$Path,
        [string]$Method = "GET",
        [string]$Body = $null
    )

    $args = @(
        "--insecure",
        "--silent",
        "--show-error",
        "--fail",
        "--max-time",
        "15",
        "--request",
        $Method,
        "$baseUrl$Path"
    )
    if ([string]::IsNullOrWhiteSpace($Body) -eq $false) {
        $args += @("--header", "Content-Type: application/json", "--data", $Body)
    }
    $response = & curl.exe @args
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

$health = Invoke-ApiJson -Path "/api/health"
$readiness = Invoke-ApiJson -Path "/api/radar/readiness"
$latestRun = Invoke-ApiJson -Path "/api/radar/runs/latest"
$activation = Invoke-ApiJson -Path "/api/radar/live-activation"
$callPlan = Invoke-ApiJson -Method "POST" -Path "/api/radar/runs/call-plan" -Body "{}"
$telemetry = Invoke-ApiJson -Path ("/api/ops/telemetry?limit={0}" -f [Math]::Max(1, $TelemetryLimit))
$telemetryCoverage = Invoke-ApiJson -Path "/api/ops/telemetry/coverage"

$payload = [ordered]@{
    health = $health
    readiness = $readiness
    latest_run = $latestRun
    live_activation = $activation
    call_plan = $callPlan
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
    Write-Output "- next safe command: powershell -ExecutionPolicy Bypass -File scripts\prepare-live-env.ps1"
    Write-Output "- after editing .env.local: powershell -ExecutionPolicy Bypass -File scripts\restart-local.ps1"
    Write-Output "- verify again: powershell -ExecutionPolicy Bypass -File scripts\check-live-activation.ps1"
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

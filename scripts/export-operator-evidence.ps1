param(
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8443,
    [int]$TelemetryLimit = 25,
    [string]$OutputPath = "",
    [switch]$Print
)

$ErrorActionPreference = "Stop"
$resolvedTelemetryLimit = [Math]::Max(1, $TelemetryLimit)
$baseUrl = "https://$ApiHost`:$ApiPort"

function Invoke-ApiJson {
    param(
        [string]$Method = "GET",
        [string]$Path,
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
        throw "Could not read local API state from $baseUrl$Path. Start services with scripts\restart-local.ps1."
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
$telemetry = Invoke-ApiJson -Path ("/api/ops/telemetry?limit={0}" -f $resolvedTelemetryLimit)
$rawTelemetry = Invoke-ApiJson -Path ("/api/ops/telemetry/raw?limit={0}" -f $resolvedTelemetryLimit)
$schwabStatus = Invoke-ApiJson -Path "/api/brokers/schwab/status"

$bundle = [ordered]@{
    schema_version = "operator-evidence-bundle-v1"
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    api_base_url = $baseUrl
    build = $health.build
    external_calls_made = 0
    summary = [ordered]@{
        api_status = $health.status
        readiness_status = $readiness.status
        safe_to_make_investment_decision = $readiness.safe_to_make_investment_decision
        readiness_next_action = $readiness.next_action
        latest_run_status = $latestRun.status
        required_completed_count = $latestRun.required_completed_count
        required_step_count = $latestRun.required_step_count
        action_needed_count = $latestRun.action_needed_count
        optional_expected_gate_count = $latestRun.optional_expected_gate_count
        live_activation_status = $activation.status
        missing_env = @($activation.missing_env)
        call_plan_status = $callPlan.status
        call_plan_next_action = $callPlan.next_action
        telemetry_status = $telemetry.status
        telemetry_attention_count = $telemetry.attention_count
        telemetry_guarded_count = $telemetry.guarded_count
        schwab_connected = $schwabStatus.connected
        schwab_order_submission_available = $schwabStatus.order_submission_available
    }
    evidence = [ordered]@{
        health = $health
        readiness = $readiness
        latest_run = $latestRun
        live_activation = $activation
        call_plan = $callPlan
        telemetry_summary = $telemetry
        raw_telemetry = $rawTelemetry
        schwab_status = $schwabStatus
    }
}

$json = $bundle | ConvertTo-Json -Depth 50

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $OutputPath = Join-Path -Path "data\ops\bundles" -ChildPath "operator-evidence-$stamp.json"
}

$parent = Split-Path -Parent $OutputPath
if ([string]::IsNullOrWhiteSpace($parent) -eq $false) {
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
}

Set-Content -Path $OutputPath -Value $json -Encoding utf8

Write-Output ("Operator evidence bundle: {0}" -f $OutputPath)
Write-Output (
    "Readiness: {0}; investable={1}; live_activation={2}; call_plan={3}" -f
    $bundle.summary.readiness_status,
    $bundle.summary.safe_to_make_investment_decision,
    $bundle.summary.live_activation_status,
    $bundle.summary.call_plan_status
)
Write-Output (
    "Telemetry: {0}; attention={1}; guarded={2}; raw_events={3}" -f
    $bundle.summary.telemetry_status,
    $bundle.summary.telemetry_attention_count,
    $bundle.summary.telemetry_guarded_count,
    $rawTelemetry.count
)
Write-Output "External calls made: 0"

if ($Print) {
    Write-Output $json
}

param(
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8443,
    [int]$TelemetryLimit = 8,
    [switch]$Json
)

$ErrorActionPreference = "Stop"
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

$readiness = Invoke-ApiJson -Path "/api/radar/readiness"
$activation = Invoke-ApiJson -Path "/api/radar/live-activation"
$callPlan = Invoke-ApiJson -Method "POST" -Path "/api/radar/runs/call-plan" -Body "{}"
$resolvedTelemetryLimit = [Math]::Max(1, $TelemetryLimit)
$telemetry = Invoke-ApiJson -Path ("/api/ops/telemetry?limit={0}" -f $resolvedTelemetryLimit)

$blockers = New-Object System.Collections.Generic.List[string]
if ($readiness.safe_to_make_investment_decision -ne $true) {
    $blockers.Add("Readiness is $($readiness.status); next=$($readiness.next_action)")
}
if ($activation.status -ne "ready") {
    $missing = @($activation.missing_env) -join ", "
    if ([string]::IsNullOrWhiteSpace($missing)) {
        $missing = "n/a"
    }
    $blockers.Add("Live activation is $($activation.status); missing=$missing")
}
if ($callPlan.status -eq "blocked") {
    $blockers.Add("Call plan is blocked; next=$($callPlan.next_action)")
}
if ($null -ne $telemetry.attention_count -and [int]$telemetry.attention_count -gt 0) {
    $blockers.Add("Telemetry has attention events; attention=$($telemetry.attention_count)")
}

$ready = $blockers.Count -eq 0
$payload = [ordered]@{
    ready = $ready
    status = $(if ($ready) { "ready" } else { "blocked" })
    safe_to_make_investment_decision = $readiness.safe_to_make_investment_decision
    readiness_status = $readiness.status
    live_activation_status = $activation.status
    call_plan_status = $callPlan.status
    telemetry_status = $telemetry.status
    telemetry_limit = $resolvedTelemetryLimit
    telemetry_attention_count = $telemetry.attention_count
    telemetry_guarded_count = $telemetry.guarded_count
    blockers = @($blockers)
    next_action = $(if ($ready) { "Run the capped live smoke, then review candidates." } else { $blockers[0] })
    external_calls_made = 0
}

if ($Json) {
    $payload | ConvertTo-Json -Depth 8
}
else {
    Write-Output ("Product readiness gate: {0}" -f $payload.status)
    Write-Output ("Investable: {0}" -f $payload.safe_to_make_investment_decision)
    Write-Output ("Readiness: {0}" -f $payload.readiness_status)
    Write-Output ("Live activation: {0}" -f $payload.live_activation_status)
    Write-Output ("Call plan: {0}" -f $payload.call_plan_status)
    Write-Output (
        "Telemetry: {0}; attention={1}; guarded={2}" -f
        $payload.telemetry_status,
        $payload.telemetry_attention_count,
        $payload.telemetry_guarded_count
    )
    if (-not $ready) {
        Write-Output "Blockers:"
        foreach ($blocker in $blockers) {
            Write-Output ("- {0}" -f $blocker)
        }
    }
    Write-Output "External calls made: 0"
}

if (-not $ready) {
    exit 1
}

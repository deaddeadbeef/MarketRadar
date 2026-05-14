param(
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8443,
    [int]$MaxUniversePages = 1,
    [int]$MaxRadarExternalCalls = 6,
    [switch]$Execute
)

$ErrorActionPreference = "Stop"
$baseUrl = "https://$ApiHost`:$ApiPort"

function Invoke-ApiJson {
    param(
        [string]$Method,
        [string]$Path,
        [object]$Body = $null
    )

    $args = @(
        "--insecure",
        "--silent",
        "--show-error",
        "--fail",
        "--max-time",
        "45",
        "--request",
        $Method,
        "$baseUrl$Path"
    )

    if ($null -ne $Body) {
        $json = $Body | ConvertTo-Json -Depth 12 -Compress
        $args += @("--header", "Content-Type: application/json", "--data", $json)
    }

    $response = & curl.exe @args
    if ($LASTEXITCODE -ne 0) {
        throw "API request failed: $Method $Path"
    }
    try {
        return $response | ConvertFrom-Json
    }
    catch {
        throw "API request returned invalid JSON: $Method $Path"
    }
}

function Write-MissingValues {
    param([object[]]$Missing)

    if ($Missing.Count -eq 0) {
        return
    }
    Write-Output ""
    Write-Output "Missing live activation values:"
    foreach ($item in $Missing) {
        Write-Output ("- {0}" -f $item)
    }
}

$activation = Invoke-ApiJson -Method "GET" -Path "/api/radar/live-activation"
Write-Output ("Live activation: {0}" -f $activation.status)
Write-Output ("Next: {0}" -f $activation.next_action)

if ($activation.status -ne "ready") {
    Write-MissingValues -Missing @($activation.missing_env)
    Write-Output ""
    Write-Output "External calls made: 0"
    Write-Output "Run scripts\prepare-live-env.ps1, fill the manual values, restart, then retry."
    exit 2
}

$plan = Invoke-ApiJson -Method "POST" -Path "/api/radar/runs/call-plan" -Body @{}
$plannedCalls = [int]$plan.max_external_call_count
Write-Output ("Radar call plan: {0}; max_external_calls={1}" -f $plan.status, $plannedCalls)

if ($plan.status -eq "blocked") {
    Write-Output "External calls made: 0"
    throw "Radar call plan is blocked; inspect /api/radar/runs/call-plan before running."
}
if ($plannedCalls -gt $MaxRadarExternalCalls) {
    Write-Output "External calls made: 0"
    throw "Radar call plan exceeds MaxRadarExternalCalls=$MaxRadarExternalCalls."
}

if (-not $Execute) {
    Write-Output ""
    Write-Output "Plan only: no provider calls were made."
    Write-Output "Re-run with -Execute to seed one capped universe page and start one capped radar cycle."
    Write-Output ("Execute budget: universe_seed_pages={0}; radar_external_calls_max={1}" -f $MaxUniversePages, $plannedCalls)
    Write-Output "Schwab and OpenAI are not called by this first-live-smoke path."
    Write-Output "External calls made: 0"
    return
}

if ($MaxUniversePages -gt 1) {
    throw "Refusing first live smoke with MaxUniversePages greater than 1."
}

Write-Output ""
Write-Output "Executing capped first live smoke."
Write-Output ("Universe seed cap: {0} Polygon page(s)." -f $MaxUniversePages)
$seed = Invoke-ApiJson -Method "POST" -Path "/api/radar/universe/seed" -Body @{
    provider = "polygon"
    max_pages = $MaxUniversePages
}
Write-Output (
    "Universe seed completed: securities={0}; rejected={1}; max_pages={2}" -f
    $seed.security_count,
    $seed.rejected_count,
    $seed.max_pages
)

$planAfterSeed = Invoke-ApiJson -Method "POST" -Path "/api/radar/runs/call-plan" -Body @{}
$plannedAfterSeedCalls = [int]$planAfterSeed.max_external_call_count
if ($planAfterSeed.status -eq "blocked") {
    throw "Radar call plan became blocked after universe seed."
}
if ($plannedAfterSeedCalls -gt $MaxRadarExternalCalls) {
    throw "Post-seed call plan exceeds MaxRadarExternalCalls=$MaxRadarExternalCalls."
}

$run = Invoke-ApiJson -Method "POST" -Path "/api/radar/runs" -Body @{}
$daily = $run.daily_result
Write-Output (
    "Radar run completed: status={0}; required={1}/{2}; optional_gates={3}" -f
    $daily.status,
    $daily.required_completed_count,
    $daily.required_step_count,
    $daily.optional_expected_gate_count
)

$readiness = Invoke-ApiJson -Method "GET" -Path "/api/radar/readiness"
Write-Output (
    "Readiness: {0}; investable={1}; next={2}" -f
    $readiness.status,
    $readiness.safe_to_make_investment_decision,
    $readiness.next_action
)
Write-Output (
    "External call budget used: universe_seed_max={0}; radar_external_calls_max={1}" -f
    $MaxUniversePages,
    $plannedAfterSeedCalls
)

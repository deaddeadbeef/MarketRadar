param(
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8443,
    [int]$MaxWorkerExternalCalls = 6,
    [switch]$Execute
)

$ErrorActionPreference = "Stop"
$baseUrl = "https://$ApiHost`:$ApiPort"
$manualGuidance = @{
    CATALYST_POLYGON_API_KEY = "Paste the Polygon API key from your Polygon dashboard."
    CATALYST_SEC_USER_AGENT = "Use a SEC-compliant contact string, for example: MarketRadar/0.1 your-email@example.com"
}

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
        $hint = $manualGuidance[$item]
        if ([string]::IsNullOrWhiteSpace($hint)) {
            Write-Output ("- {0}" -f $item)
        }
        else {
            Write-Output ("- {0}: {1}" -f $item, $hint)
        }
    }
}

$activation = Invoke-ApiJson -Method "GET" -Path "/api/radar/live-activation"
Write-Output ("Live activation: {0}" -f $activation.status)
Write-Output ("Next: {0}" -f $activation.next_action)

if ($activation.status -ne "ready") {
    Write-MissingValues -Missing @($activation.missing_env)
    Write-Output ""
    Write-Output "Worker one-shot call plan: blocked; external_providers=False; max_external_calls=0"
    Write-Output "External calls made: 0"
    Write-Output "Run scripts\prepare-live-env.ps1, fill the manual values, restart, then retry."
    exit 2
}

$plan = Invoke-ApiJson -Method "POST" -Path "/api/radar/runs/call-plan" -Body @{}
$plannedCalls = [int]$plan.max_external_call_count
Write-Output (
    "Worker one-shot call plan: {0}; external_providers={1}; max_external_calls={2}" -f
    $plan.status,
    $plan.will_call_external_providers,
    $plannedCalls
)

if ($plan.status -eq "blocked") {
    Write-Output "External calls made: 0"
    Write-Output "Worker call plan is blocked; inspect /api/radar/runs/call-plan before running."
    exit 2
}
if ($plannedCalls -gt $MaxWorkerExternalCalls) {
    Write-Output "External calls made: 0"
    Write-Output "Worker call plan exceeds MaxWorkerExternalCalls=$MaxWorkerExternalCalls."
    exit 3
}

if (-not $Execute) {
    Write-Output ""
    Write-Output "Plan only: no provider calls were made and the worker was not started."
    Write-Output "Re-run with -Execute to run exactly one worker cycle with this call budget."
    Write-Output (
        "Execute budget: worker_external_calls_max={0}; Schwab calls=0; OpenAI calls=0" -f
        $plannedCalls
    )
    Write-Output "External calls made: 0"
    return
}

Write-Output ""
Write-Output "Executing one worker cycle."
$env:CATALYST_WORKER_INTERVAL_SECONDS = "0"
$env:CATALYST_RUN_LLM = "false"
$env:CATALYST_LLM_DRY_RUN = "true"
$env:CATALYST_DRY_RUN_ALERTS = "true"
python -m apps.worker.main
$workerExitCode = $LASTEXITCODE
if ($workerExitCode -ne 0) {
    throw "Worker exited with code $workerExitCode."
}

$latest = Invoke-ApiJson -Method "GET" -Path "/api/radar/runs/latest"
Write-Output (
    "Latest run: {0}; required={1}/{2}; action_needed={3}; optional_gates={4}" -f
    $latest.status,
    $latest.required_completed_count,
    $latest.required_step_count,
    $latest.action_needed_count,
    $latest.optional_expected_gate_count
)
Write-Output (
    "External call budget allowed: worker_external_calls_max={0}" -f
    $plannedCalls
)

param(
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8443,
    [switch]$Json
)

$ErrorActionPreference = "Stop"

$uri = "https://$ApiHost`:$ApiPort/api/radar/live-activation"
$responseBody = & curl.exe --insecure --silent --show-error --fail --max-time 10 $uri
if ($LASTEXITCODE -ne 0) {
    throw "Could not read live activation status from $uri. Start the local API first with scripts\restart-local.ps1."
}

if ($Json) {
    $responseBody
    return
}

try {
    $payload = $responseBody | ConvertFrom-Json
}
catch {
    throw "Live activation endpoint returned invalid JSON."
}

Write-Output ("Live activation: {0}" -f $payload.status)
Write-Output ("Headline: {0}" -f $payload.headline)
Write-Output ("Next: {0}" -f $payload.next_action)
Write-Output ("External calls made by this check: 0")
Write-Output ""

$dotenv = $payload.dotenv_file
if ($null -ne $dotenv) {
    Write-Output (
        ".env.local: {0}; loaded={1}; missing={2}; restart_required={3}" -f
        $dotenv.status,
        $dotenv.loaded_count,
        $dotenv.missing_count,
        $dotenv.restart_required_count
    )
}

$missing = @($payload.missing_env)
if ($missing.Count -gt 0) {
    Write-Output ""
    Write-Output "Missing live activation values:"
    foreach ($item in $missing) {
        Write-Output ("- {0}" -f $item)
    }
}

$steps = @($payload.operator_steps)
if ($steps.Count -gt 0) {
    Write-Output ""
    Write-Output "Safe next steps:"
    foreach ($step in $steps) {
        Write-Output (
            "{0}. [{1}] {2} (external calls: {3})" -f
            $step.step,
            $step.status,
            $step.action,
            $step.external_calls
        )
        if ($null -ne $step.command -and [string]::IsNullOrWhiteSpace($step.command) -eq $false) {
            Write-Output ("   {0}" -f $step.command)
        }
    }
}

Write-Output ""
Write-Output "Zero-call verification commands:"
Write-Output ("- Readiness: curl.exe --insecure --fail --silent --show-error --request GET https://{0}:{1}/api/radar/readiness" -f $ApiHost, $ApiPort)
Write-Output ("- Latest run: curl.exe --insecure --fail --silent --show-error --request GET https://{0}:{1}/api/radar/runs/latest" -f $ApiHost, $ApiPort)
Write-Output ("- Telemetry: curl.exe --insecure --fail --silent --show-error --request GET https://{0}:{1}/api/ops/telemetry?limit=8" -f $ApiHost, $ApiPort)
Write-Output ("- Call plan: curl.exe --insecure --fail --silent --show-error --request POST https://{0}:{1}/api/radar/runs/call-plan --header ""Content-Type: application/json"" --data '{{}}'" -f $ApiHost, $ApiPort)
Write-Output "These commands read local API state only; they do not call Polygon, SEC, Schwab, or OpenAI."

param(
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8443,
    [string]$Month = "",
    [string]$AvailableAt = "",
    [switch]$Json
)

$ErrorActionPreference = "Stop"
$baseUrl = "https://$ApiHost`:$ApiPort"

function Invoke-ApiJson {
    param(
        [string]$Path
    )

    $response = & curl.exe `
        --insecure `
        --silent `
        --show-error `
        --fail `
        --max-time 15 `
        --request GET `
        "$baseUrl$Path"
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

function Add-QueryPart {
    param(
        [System.Collections.Generic.List[string]]$Parts,
        [string]$Name,
        [string]$Value
    )
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return
    }
    $Parts.Add(("{0}={1}" -f $Name, [Uri]::EscapeDataString($Value)))
}

$query = [System.Collections.Generic.List[string]]::new()
Add-QueryPart -Parts $query -Name "month" -Value $Month
Add-QueryPart -Parts $query -Name "available_at" -Value $AvailableAt
$path = "/api/radar/investable/readiness"
if ($query.Count -gt 0) {
    $path = "{0}?{1}" -f $path, ($query -join "&")
}

$payload = Invoke-ApiJson -Path $path

if ($Json) {
    $payload | ConvertTo-Json -Depth 50
}
else {
    Write-Output ("Investable readiness gate: {0}" -f $payload.status)
    Write-Output ("Ready: {0}" -f $payload.ready)
    Write-Output ("Decision support only: {0}" -f $payload.decision_support_only)
    Write-Output ("Highest allowed action: {0}" -f $payload.highest_allowed_action_state)
    Write-Output ("Limited-capital pilot: {0}" -f $payload.limited_capital_pilot_status)
    Write-Output ("Next: {0}" -f $payload.canonical_next_action)
    if ($payload.ready -ne $true) {
        Write-Output "Blockers:"
        foreach ($blocker in @($payload.blockers)) {
            Write-Output ("- {0}: {1}" -f $blocker.code, $blocker.finding)
        }
    }
    Write-Output ("External calls made: {0}" -f $payload.external_calls_made)
    Write-Output ("DB writes made: {0}" -f $payload.db_writes_made)
}

if ($payload.ready -ne $true) {
    exit 1
}

param(
    [string]$EnvPath = ".env.local",
    [string]$ExamplePath = ".env.example",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $ExamplePath)) {
    throw "Could not find $ExamplePath."
}

if (Test-Path -LiteralPath $EnvPath) {
    $lines = @(Get-Content -LiteralPath $EnvPath)
    $created = $false
}
else {
    $lines = @(Get-Content -LiteralPath $ExamplePath)
    $created = $true
}

$safeDefaults = [ordered]@{
    CATALYST_DAILY_MARKET_PROVIDER = "polygon"
    CATALYST_DAILY_PROVIDER = "polygon"
    CATALYST_POLYGON_TICKERS_MAX_PAGES = "1"
    CATALYST_DAILY_EVENT_PROVIDER = "sec"
    CATALYST_SEC_ENABLE_LIVE = "1"
    CATALYST_SEC_DAILY_MAX_TICKERS = "5"
    CATALYST_RADAR_RUN_MIN_INTERVAL_SECONDS = "300"
    CATALYST_RUN_LLM = "false"
    CATALYST_LLM_DRY_RUN = "true"
    CATALYST_DRY_RUN_ALERTS = "true"
    SCHWAB_ORDER_SUBMISSION_ENABLED = "false"
}

$manualValues = @(
    "CATALYST_POLYGON_API_KEY",
    "CATALYST_SEC_USER_AGENT"
)

function Set-EnvLine {
    param(
        [string[]]$InputLines,
        [string]$Key,
        [string]$Value
    )

    $pattern = "^\s*$([regex]::Escape($Key))\s*="
    $updated = $false
    $result = foreach ($line in $InputLines) {
        if (-not $updated -and $line -match $pattern) {
            $updated = $true
            "$Key=$Value"
        }
        else {
            $line
        }
    }
    if (-not $updated) {
        $result += "$Key=$Value"
    }
    return @($result)
}

function Get-EnvValue {
    param(
        [string[]]$InputLines,
        [string]$Key
    )

    $pattern = "^\s*$([regex]::Escape($Key))\s*=(.*)$"
    foreach ($line in $InputLines) {
        if ($line -match $pattern) {
            return [string]$Matches[1].Trim()
        }
    }
    return ""
}

function Test-EnvKey {
    param(
        [string[]]$InputLines,
        [string]$Key
    )

    $pattern = "^\s*$([regex]::Escape($Key))\s*="
    foreach ($line in $InputLines) {
        if ($line -match $pattern) {
            return $true
        }
    }
    return $false
}

foreach ($entry in $safeDefaults.GetEnumerator()) {
    $lines = Set-EnvLine -InputLines $lines -Key $entry.Key -Value $entry.Value
}

$missingManual = @()
foreach ($key in $manualValues) {
    if (-not (Test-EnvKey -InputLines $lines -Key $key)) {
        $lines = Set-EnvLine -InputLines $lines -Key $key -Value ""
    }
    if ([string]::IsNullOrWhiteSpace((Get-EnvValue -InputLines $lines -Key $key))) {
        $missingManual += $key
    }
}

if ($DryRun) {
    Write-Output "Dry run: no files were written."
}
else {
    $parent = Split-Path -Parent $EnvPath
    if (-not [string]::IsNullOrWhiteSpace($parent) -and -not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Path $parent | Out-Null
    }
    Set-Content -LiteralPath $EnvPath -Value $lines -Encoding UTF8
}

if ($created) {
    Write-Output ("Created {0} from {1}." -f $EnvPath, $ExamplePath)
}
else {
    Write-Output ("Updated safe live defaults in {0}." -f $EnvPath)
}
Write-Output "External calls made by this script: 0"
Write-Output "Order submission remains disabled."

if ($missingManual.Count -gt 0) {
    Write-Output ""
    Write-Output "Fill these values manually before the first live run:"
    foreach ($key in $missingManual) {
        Write-Output ("- {0}" -f $key)
    }
}

Write-Output ""
Write-Output "After filling manual values:"
Write-Output "1. powershell -ExecutionPolicy Bypass -File scripts\restart-local.ps1"
Write-Output "2. powershell -ExecutionPolicy Bypass -File scripts\check-live-activation.ps1"

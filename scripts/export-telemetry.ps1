param(
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8443,
    [int]$Limit = 100,
    [string]$OutputPath = "",
    [switch]$Print
)

$ErrorActionPreference = "Stop"
$resolvedLimit = [Math]::Max(1, $Limit)
$baseUrl = "https://$ApiHost`:$ApiPort"
$path = "/api/ops/telemetry/raw?limit=$resolvedLimit"

$response = & curl.exe `
    --insecure `
    --silent `
    --show-error `
    --fail `
    --max-time 15 `
    "$baseUrl$path"
if ($LASTEXITCODE -ne 0) {
    throw "Could not export telemetry from $baseUrl$path. Start services with scripts\restart-local.ps1."
}

try {
    $payload = $response | ConvertFrom-Json
}
catch {
    throw "Local API returned invalid telemetry export JSON."
}

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $OutputPath = Join-Path -Path "data\ops\telemetry" -ChildPath "telemetry-export-$stamp.json"
}

$parent = Split-Path -Parent $OutputPath
if ([string]::IsNullOrWhiteSpace($parent) -eq $false) {
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
}

Set-Content -Path $OutputPath -Value $response -Encoding utf8

Write-Output ("Telemetry export: {0}" -f $OutputPath)
Write-Output ("Events: {0}; limit={1}" -f $payload.count, $payload.limit)
Write-Output "External calls made: 0"

if ($Print) {
    Write-Output $response
}

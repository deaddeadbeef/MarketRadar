#Requires -Version 5.1
<#
.SYNOPSIS
  Install a fresh world-events-v1 JSON into the path the main app reads, then
  smoke-check discovery-brief so World Events stays in sync.

.DESCRIPTION
  The desktop / discovery-snapshot always load data/local/world_events.json
  (falling back to data/sample). Stale next-actions must not suggest
  --validate-only — that never updates the feed.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts/refresh-world-events.ps1 `
    -EventsPath data\local\inbox\world_events.json -Execute

  powershell -ExecutionPolicy Bypass -File scripts/refresh-world-events.ps1 -Execute
  # Re-validates existing data/local/world_events.json and prints brief status.
#>
[CmdletBinding()]
param(
  [string]$EventsPath = "",
  [string]$Destination = "data\local\world_events.json",
  [switch]$Execute,
  [switch]$FanoutEvents
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
  $python = "python"
}
$env:PYTHONPATH = "src"

$destResolved = Join-Path $repoRoot $Destination
if ([System.IO.Path]::IsPathRooted($Destination)) {
  $destResolved = $Destination
}

$source = $EventsPath
if ([string]::IsNullOrWhiteSpace($source)) {
  Write-Error @"
Refusing to validate-only the installed feed. That never refreshes world events.

Provide a fresh world-events-v1 JSON, for example:
  powershell -ExecutionPolicy Bypass -File scripts/refresh-world-events.ps1 ``
    -EventsPath path\to\world_events.json -Execute

The main app reads: $Destination
"@
}

if (-not [System.IO.Path]::IsPathRooted($source)) {
  $source = Join-Path $repoRoot $source
}
if (-not (Test-Path $source)) {
  Write-Error "Events file not found: $source"
}

$samePath = (Resolve-Path $source).Path -eq (Resolve-Path (Split-Path -Parent $destResolved) -ErrorAction SilentlyContinue).Path
try {
  $srcFull = (Resolve-Path $source).Path
  $destDir = Split-Path -Parent $destResolved
  if (-not (Test-Path $destDir)) {
    New-Item -ItemType Directory -Path $destDir -Force | Out-Null
  }
  $destFull = if (Test-Path $destResolved) { (Resolve-Path $destResolved).Path } else { $destResolved }
  $samePath = $srcFull -eq $destFull
} catch {
  $samePath = $false
}

if (-not $samePath) {
  $cliArgs = @(
    "-m", "catalyst_radar.cli",
    "discovery-ingest",
    "--events", $source,
    "--destination", $Destination,
    "--json"
  )
  if ($FanoutEvents) { $cliArgs += "--fanout-events" }
  if ($Execute) {
    $cliArgs += "--execute"
  } else {
    Write-Host "Preview mode (no install). Pass -Execute to write $Destination."
  }
  Write-Host "Running: $python $($cliArgs -join ' ')"
  & $python @cliArgs
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  if (-not $Execute) {
    Write-Host "Preview only. Re-run with -Execute after reviewing validation."
    exit 0
  }
} else {
  # Already at destination — validate in place.
  $cliArgs = @(
    "-m", "catalyst_radar.cli",
    "discovery-ingest",
    "--events", $source,
    "--validate-only",
    "--json"
  )
  Write-Host "Events already at install path; validating: $source"
  & $python @cliArgs
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  if (-not $Execute) {
    Write-Host "Validation OK. Pass -Execute to run discovery-brief smoke-check."
    exit 0
  }
}

# Smoke-check the product path the main app uses.
Write-Host ""
Write-Host "Discovery brief (what World Events will show):"
& $python -m catalyst_radar.cli discovery-brief --events $Destination --json
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "Installed. Refresh the desktop (R / F5) or relaunch Open-MarketRadar.bat."
exit 0

#Requires -Version 5.1
<#
.SYNOPSIS
  Validate and install a world-events-v1 JSON file for MarketRadar discovery.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts/import-world-events.ps1
  powershell -ExecutionPolicy Bypass -File scripts/import-world-events.ps1 -EventsPath .\data\sample\world_events.json -Execute
  powershell -ExecutionPolicy Bypass -File scripts/import-world-events.ps1 -Execute -FanoutEvents
#>
[CmdletBinding()]
param(
  [string]$EventsPath = "data\sample\world_events.json",
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
$cliArgs = @(
  "-m", "catalyst_radar.cli",
  "discovery-ingest",
  "--events", $EventsPath,
  "--destination", $Destination,
  "--json"
)
if ($FanoutEvents) {
  $cliArgs += "--fanout-events"
}
if ($Execute) {
  $cliArgs += "--execute"
}

Write-Host "Running: $python $($cliArgs -join ' ')"
& $python @cliArgs
exit $LASTEXITCODE

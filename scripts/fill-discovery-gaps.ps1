#Requires -Version 5.1
<#
.SYNOPSIS
  Operator leftover around discovery join gaps.

  Discovery bar path: catalyst-radar discovery-bars --polygon --confirm-external-call
  Grouped-daily capture here is already skipped_full_market_not_discovery.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts/fill-discovery-gaps.ps1
  powershell -ExecutionPolicy Bypass -File scripts/fill-discovery-gaps.ps1 -Execute -ConfirmExternalCall -CaptureDays 8
#>
[CmdletBinding()]
param(
  [switch]$Execute,
  [switch]$ConfirmExternalCall,
  [int]$CaptureDays = 8,
  [string]$EventsPath = "data\local\world_events.json",
  [string]$AsOf = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { $python = "python" }

$env:PYTHONPATH = "src"
$cliArgs = @(
  "scripts\fill_discovery_gaps.py",
  "--capture-days", [string]$CaptureDays,
  "--events", $EventsPath,
  "--json"
)
if ($Execute) { $cliArgs += "--execute" }
if ($ConfirmExternalCall) { $cliArgs += "--confirm-external-call" }
if ($AsOf) { $cliArgs += @("--as-of", $AsOf) }

Write-Host "fill-discovery-gaps: repo=$repoRoot execute=$Execute external=$ConfirmExternalCall capture_days=$CaptureDays"
& $python @cliArgs
exit $LASTEXITCODE

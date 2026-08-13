#Requires -Version 5.1
<#
.SYNOPSIS
  Launch the product desktop app on World Events with the discovery snapshot.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$desktop = Join-Path $repoRoot "target\release\radar-desktop.exe"
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { $python = "python" }
$snapshot = Join-Path $repoRoot "scripts\discovery-snapshot.py"

if (-not (Test-Path $desktop)) {
  Write-Error "Missing $desktop. Build with: cargo build -p radar-desktop --release"
}

$snapshotCommand = "& '$python' '$snapshot'"
Start-Process -FilePath $desktop -WorkingDirectory $repoRoot -ArgumentList @(
  "--page", "world-events",
  "--snapshot-command", $snapshotCommand
)
Write-Host "Launched MarketRadar World Events."

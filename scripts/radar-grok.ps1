#Requires -Version 5.1
<#
.SYNOPSIS
  Headless Grok/CLI helpers for MarketRadar (hunt, brief, convert, ready, bars, status).
  The product receive surface is catalyst-radar JSON, not the desktop GUI.
#>
[CmdletBinding()]
param(
  [Parameter(Position = 0)]
  [ValidateSet("hunt", "brief", "convert", "ready", "bars", "status")]
  [string]$Action = "brief",
  [string]$PostsPath = "",
  [switch]$Execute,
  [switch]$ConfirmExternalCall
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$pythonWin = Join-Path $repoRoot ".venv\Scripts\python.exe"
$pythonUnix = Join-Path $repoRoot ".venv/bin/python"
if (Test-Path $pythonWin) { $python = $pythonWin }
elseif (Test-Path $pythonUnix) { $python = $pythonUnix }
else { $python = "python3" }
$env:PYTHONPATH = Join-Path $repoRoot "src"
$helper = Join-Path $PSScriptRoot "radar_grok.py"

switch ($Action) {
  "status" { & $python $helper status; exit $LASTEXITCODE }
  "hunt" { & $python $helper hunt; exit $LASTEXITCODE }
  "brief" { & $python $helper brief; exit $LASTEXITCODE }
  "convert" {
    if ([string]::IsNullOrWhiteSpace($PostsPath)) {
      $today = Get-Date -Format "yyyy-MM-dd"
      $PostsPath = Join-Path $repoRoot "data\local\inbox\x_posts_$today.json"
    }
    $args = @($helper, "convert", "--posts", $PostsPath)
    if ($Execute) { $args += "--execute" }
    & $python @args
    exit $LASTEXITCODE
  }
  "ready" { & $python $helper ready; exit $LASTEXITCODE }
  "bars" {
    $args = @($helper, "bars")
    if ($ConfirmExternalCall) { $args += "--confirm-external-call" }
    if ($Execute) { $args += "--execute" }
    & $python @args
    exit $LASTEXITCODE
  }
}

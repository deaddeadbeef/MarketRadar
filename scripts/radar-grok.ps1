#Requires -Version 5.1
<#
.SYNOPSIS
  Grok Build entrypoints for MarketRadar (brief, convert, ready, bars, status, open).
#>
[CmdletBinding()]
param(
  [Parameter(Position = 0)]
  [ValidateSet("brief", "convert", "ready", "bars", "status", "open")]
  [string]$Action = "brief",
  [string]$PostsPath = "",
  [switch]$Execute,
  [switch]$ConfirmExternalCall
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { $python = "python" }
$env:PYTHONPATH = Join-Path $repoRoot "src"
$helper = Join-Path $PSScriptRoot "radar_grok.py"

switch ($Action) {
  "status" { & $python $helper status; exit $LASTEXITCODE }
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
  "open" {
    & (Join-Path $PSScriptRoot "open-market-radar.ps1")
    exit $LASTEXITCODE
  }
}

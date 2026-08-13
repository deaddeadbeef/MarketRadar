#Requires -Version 5.1
<#
.SYNOPSIS
  Real-data discovery loop: install today's posts, fetch mapped Polygon bars, print insights.
#>
[CmdletBinding()]
param(
  [string]$PostsPath = "data\sample\x_posts_2026-08-13.json",
  [switch]$Execute,
  [switch]$ConfirmExternalCall
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { $python = "python" }
$env:PYTHONPATH = "src"

$dest = "data\local\world_events.json"
$fromPosts = @(
  "-m", "catalyst_radar.cli", "discovery-from-posts",
  "--posts", $PostsPath, "--destination", $dest, "--json"
)
if ($Execute) { $fromPosts += "--execute" }
& $python @fromPosts
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$bars = @(
  "-m", "catalyst_radar.cli", "discovery-bars",
  "--polygon", "--events", $dest, "--json"
)
if ($ConfirmExternalCall) { $bars += "--confirm-external-call" }
if ($Execute) { $bars += "--execute" }
& $python @bars
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if ($Execute) {
  & $python -m catalyst_radar.cli discovery-insights --events $dest --persist
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  & $python -m catalyst_radar.cli assert-discovery-ready --events $dest --json
  exit $LASTEXITCODE
}
Write-Host "Preview only. Re-run with -Execute -ConfirmExternalCall to fetch bars and print insights."
exit 0

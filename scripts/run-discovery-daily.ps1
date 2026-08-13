#Requires -Version 5.1
<#
.SYNOPSIS
  Daily event-first loop: install events if given, persist brief, print readiness.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File scripts/run-discovery-daily.ps1 `
    -EventsPath data\local\inbox\world_events.json -Execute
#>
[CmdletBinding()]
param(
  [string]$EventsPath = "",
  [string]$PostsPath = "",
  [string]$BarsCsv = "",
  [switch]$Execute
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { $python = "python" }
$env:PYTHONPATH = "src"

if (-not [string]::IsNullOrWhiteSpace($PostsPath)) {
  $fromPosts = @(
    "-m", "catalyst_radar.cli",
    "discovery-from-posts",
    "--posts", $PostsPath,
    "--destination", "data\local\world_events.json",
    "--json"
  )
  if ($Execute) { $fromPosts += "--execute" }
  & $python @fromPosts
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

if (-not [string]::IsNullOrWhiteSpace($BarsCsv)) {
  $bars = @(
    "-m", "catalyst_radar.cli",
    "discovery-bars",
    "--csv", $BarsCsv,
    "--json"
  )
  if ($Execute) { $bars += "--execute" }
  & $python @bars
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

if (-not [string]::IsNullOrWhiteSpace($EventsPath)) {
  $refresh = @(
    "-ExecutionPolicy", "Bypass",
    "-File", (Join-Path $repoRoot "scripts\refresh-world-events.ps1"),
    "-EventsPath", $EventsPath
  )
  if ($Execute) { $refresh += "-Execute" }
  & powershell @refresh
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

$briefArgs = @("-m", "catalyst_radar.cli", "discovery-brief", "--json")
if ($Execute) { $briefArgs += "--persist" }
& $python @briefArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& $python -m catalyst_radar.cli assert-discovery-ready --json
exit $LASTEXITCODE

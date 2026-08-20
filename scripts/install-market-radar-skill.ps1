#Requires -Version 5.1
<#
.SYNOPSIS
  Install the repo MarketRadar skill into Grok's user skills folder.
#>
[CmdletBinding()]
param(
    [string]$RepoRoot = "",
    [string]$GrokHome = "",
    [string]$LiveInstall = "C:\Users\fpan1\MarketRadar"
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}
if ([string]::IsNullOrWhiteSpace($GrokHome)) {
    if ($env:GROK_HOME) { $GrokHome = $env:GROK_HOME }
    else { $GrokHome = Join-Path $env:USERPROFILE ".grok" }
}

$src = Join-Path $RepoRoot ".grok\skills\market-radar"
$dest = Join-Path $GrokHome "skills\market-radar"
$openSrc = Join-Path $RepoRoot "scripts\open-market-radar.ps1"

if (-not (Test-Path -LiteralPath (Join-Path $src "SKILL.md"))) {
    throw "Skill source missing: $src\SKILL.md"
}
if (-not (Test-Path -LiteralPath $openSrc)) {
    throw "Launcher missing: $openSrc"
}

New-Item -ItemType Directory -Force -Path (Join-Path $dest "references") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $dest "scripts") | Out-Null
Copy-Item -LiteralPath (Join-Path $src "SKILL.md") -Destination (Join-Path $dest "SKILL.md") -Force
$hunt = Join-Path $src "references\hunt.md"
if (Test-Path -LiteralPath $hunt) {
    Copy-Item -LiteralPath $hunt -Destination (Join-Path $dest "references\hunt.md") -Force
}
Copy-Item -LiteralPath $openSrc -Destination (Join-Path $dest "scripts\open-market-radar.ps1") -Force
$stale = Join-Path $dest "scripts\open-world-events.ps1"
if (Test-Path -LiteralPath $stale) {
    Remove-Item -LiteralPath $stale -Force
}

$liveOpen = Join-Path $LiveInstall "scripts\open-market-radar.ps1"
if ((Test-Path -LiteralPath (Join-Path $LiveInstall "scripts")) -and ($openSrc -ne $liveOpen)) {
    Copy-Item -LiteralPath $openSrc -Destination $liveOpen -Force
}

$payload = [ordered]@{
    schema_version = "market-radar-skill-install-v1"
    status = "installed"
    source = $src
    destination = $dest
    grok_home = $GrokHome
    live_install = $LiveInstall
    files = @(
        (Join-Path $dest "SKILL.md"),
        (Join-Path $dest "references\hunt.md"),
        (Join-Path $dest "scripts\open-market-radar.ps1")
    )
}
$payload | ConvertTo-Json -Compress
exit 0

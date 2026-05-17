param(
    [string]$ProfilePath = $PROFILE,
    [string]$RepoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
)

$ErrorActionPreference = "Stop"

$launcher = Join-Path $RepoRoot "scripts\run-dashboard-tui.ps1"
if (-not (Test-Path -LiteralPath $launcher)) {
    throw "Dashboard launcher not found: $launcher"
}

$startMarker = "# >>> MarketRadar dashboard alias >>>"
$endMarker = "# <<< MarketRadar dashboard alias <<<"
$block = @"
$startMarker
function market-radar {
    & "$launcher" @args
}
Set-Alias -Name radar -Value market-radar
$endMarker
"@

$profileDir = Split-Path -Parent $ProfilePath
if (-not [string]::IsNullOrWhiteSpace($profileDir)) {
    New-Item -ItemType Directory -Force -Path $profileDir | Out-Null
}

$content = ""
if (Test-Path -LiteralPath $ProfilePath) {
    $content = Get-Content -LiteralPath $ProfilePath -Raw
}

$startIndex = $content.IndexOf($startMarker)
$endIndex = $content.IndexOf($endMarker)
if ($startIndex -ge 0 -and $endIndex -gt $startIndex) {
    $afterEnd = $endIndex + $endMarker.Length
    $content = $content.Remove($startIndex, $afterEnd - $startIndex)
}

$trimmed = $content.TrimEnd()
$newContent = if ([string]::IsNullOrWhiteSpace($trimmed)) {
    $block
}
else {
    "$trimmed`r`n`r`n$block"
}

Set-Content -LiteralPath $ProfilePath -Value $newContent -Encoding utf8

Write-Output "Installed MarketRadar dashboard alias in $ProfilePath"
Write-Output "Open a new PowerShell session, then run: radar"
Write-Output "Pass dashboard args directly, for example: radar --once --page overview"
Write-Output "Skip auto-update when needed: radar --no-update"

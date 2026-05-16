param(
    [string]$EnvPath = ".env.local",
    [string]$ExamplePath = ".env.example",
    [switch]$NoPrepare
)

$ErrorActionPreference = "Stop"

$prepareScript = Join-Path $PSScriptRoot "prepare-live-env.ps1"
if (-not (Test-Path -LiteralPath $prepareScript)) {
    throw "Could not find scripts\prepare-live-env.ps1."
}

if (-not $NoPrepare) {
    & $prepareScript -EnvPath $EnvPath -ExamplePath $ExamplePath -Quiet
}

if (-not (Test-Path -LiteralPath $EnvPath)) {
    throw "Could not find $EnvPath after preparing live env defaults."
}

$resolvedEnvPath = (Resolve-Path -LiteralPath $EnvPath).Path
$codeCommand = Get-Command code.cmd -ErrorAction SilentlyContinue
if ($null -eq $codeCommand) {
    $codeCommand = Get-Command code -ErrorAction SilentlyContinue
}

if ($null -ne $codeCommand) {
    Start-Process -FilePath $codeCommand.Source -ArgumentList @("--reuse-window", $resolvedEnvPath)
    Write-Output ("Opened {0} in VS Code." -f $resolvedEnvPath)
}
else {
    Start-Process -FilePath "notepad.exe" -ArgumentList $resolvedEnvPath
    Write-Output ("Opened {0} in Notepad because VS Code was not found on PATH." -f $resolvedEnvPath)
}

Write-Output "External calls made by this script: 0"
Write-Output "Safe live defaults are prepared; order submission remains disabled."
Write-Output "Fill CATALYST_SEC_USER_AGENT manually; Polygon is optional and only needed if you switch the market provider to polygon."
Write-Output ""
Write-Output "After filling manual values:"
Write-Output "1. powershell -ExecutionPolicy Bypass -File scripts\restart-local.ps1"
Write-Output "2. powershell -ExecutionPolicy Bypass -File scripts\check-live-activation.ps1"
Write-Output "3. powershell -ExecutionPolicy Bypass -File scripts\run-first-live-smoke.ps1"
Write-Output "4. Only if the plan-only smoke matches intent: powershell -ExecutionPolicy Bypass -File scripts\run-first-live-smoke.ps1 -Execute"

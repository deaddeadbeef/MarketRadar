param(
    [string]$Page = "tutorial",
    [int]$TimeoutSeconds = 120
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
$launcher = Join-Path $repoRoot "scripts\run-dashboard-tui.ps1"

function ConvertTo-ProcessArgument {
    param([AllowNull()][string]$Argument)

    if ($null -eq $Argument -or $Argument.Length -eq 0) {
        return '""'
    }
    if ($Argument -notmatch '[\s"]') {
        return $Argument
    }
    return '"' + ($Argument -replace '"', '\"') + '"'
}

function Get-ChildProcessIds {
    param([int]$ProcessId)

    $children = @(
        Get-CimInstance Win32_Process -Filter "ParentProcessId=$ProcessId" `
            -ErrorAction SilentlyContinue
    )
    foreach ($child in $children) {
        Get-ChildProcessIds -ProcessId ([int]$child.ProcessId)
        [int]$child.ProcessId
    }
}

function Stop-ProcessTree {
    param([System.Diagnostics.Process]$Process)

    if ($null -eq $Process) {
        return
    }
    foreach ($childId in @(Get-ChildProcessIds -ProcessId $Process.Id)) {
        Stop-Process -Id $childId -Force -ErrorAction SilentlyContinue
    }
    if (-not $Process.HasExited) {
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        $Process.WaitForExit(5000) | Out-Null
    }
}

function Get-DashboardProcessSnapshot {
    Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object {
            $_.CommandLine -like "*dashboard-tui*" -and
            $_.CommandLine -like "*MarketRadar*" -and
            $_.CommandLine -notlike "*debug-dashboard-e2e.ps1*"
        } |
        Select-Object ProcessId, ParentProcessId, CommandLine
}

function Get-CurrentPowerShellPath {
    $current = Get-Process -Id $PID -ErrorAction SilentlyContinue
    if ($null -ne $current -and -not [string]::IsNullOrWhiteSpace($current.Path)) {
        return $current.Path
    }
    $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($null -ne $pwsh) {
        return $pwsh.Source
    }
    return "powershell.exe"
}

Write-Output "MarketRadar dashboard end-to-end debug"
Write-Output "Repo: $repoRoot"
Write-Output "Page: $Page"
Write-Output "External calls made: 0"

$radarCommand = Get-Command radar -ErrorAction SilentlyContinue
if ($null -ne $radarCommand) {
    Write-Output "radar command: $($radarCommand.Definition)"
}
else {
    Write-Output "radar command: not found in this PowerShell session"
}

$before = @(Get-DashboardProcessSnapshot)
Write-Output "dashboard-tui processes before: $($before.Count)"

$powerShellExe = Get-CurrentPowerShellPath
$arguments = @(
    "-NoLogo",
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    $launcher,
    "--no-update",
    "--once",
    "--page",
    $Page
)

$argumentLine = ($arguments | ForEach-Object { ConvertTo-ProcessArgument $_ }) -join " "

$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$stdoutPath = Join-Path $env:TEMP ("marketradar-dashboard-e2e-{0}.out" -f ([guid]::NewGuid()))
$stderrPath = Join-Path $env:TEMP ("marketradar-dashboard-e2e-{0}.err" -f ([guid]::NewGuid()))
$process = Start-Process `
    -FilePath $powerShellExe `
    -ArgumentList $argumentLine `
    -WorkingDirectory ([string]$repoRoot) `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath `
    -WindowStyle Hidden `
    -PassThru

try {
    if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
        Stop-ProcessTree -Process $process
        Write-Error "Dashboard E2E timed out after $TimeoutSeconds seconds."
        exit 2
    }
    $process.WaitForExit()
    $process.Refresh()
    $exitCode = [int]$process.ExitCode
    $stdout = if (Test-Path -LiteralPath $stdoutPath) {
        Get-Content -LiteralPath $stdoutPath -Raw
    }
    else {
        ""
    }
    $stderr = if (Test-Path -LiteralPath $stderrPath) {
        Get-Content -LiteralPath $stderrPath -Raw
    }
    else {
        ""
    }
}
finally {
    Stop-ProcessTree -Process $process
    Remove-Item -LiteralPath $stdoutPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $stderrPath -Force -ErrorAction SilentlyContinue
}
$stopwatch.Stop()

Write-Output ("launcher exit code: {0}" -f $exitCode)
Write-Output ("elapsed seconds: {0:n1}" -f $stopwatch.Elapsed.TotalSeconds)
if (-not [string]::IsNullOrWhiteSpace($stderr)) {
    Write-Output "stderr:"
    Write-Output $stderr
}

if ($exitCode -ne 0) {
    Write-Output "stdout:"
    Write-Output $stdout
    exit $exitCode
}

if (
    $stdout -notmatch "Market Radar Terminal Dashboard" -and
    $stdout -notmatch "Tutorial - your first 90 seconds" -and
    $stdout -notmatch "MARKET RADAR"
) {
    Write-Output "stdout:"
    Write-Output $stdout
    Write-Error "Dashboard output did not contain an expected dashboard marker."
    exit 3
}

if ($stdout -notmatch "External calls made:\s*0") {
    Write-Output "stdout:"
    Write-Output $stdout
    Write-Error "Dashboard debug smoke did not prove zero external calls."
    exit 4
}

$after = @(Get-DashboardProcessSnapshot)
$beforeIds = @{}
foreach ($row in $before) {
    $beforeIds[[int]$row.ProcessId] = $true
}
$newLeftovers = @(
    $after | Where-Object { -not $beforeIds.ContainsKey([int]$_.ProcessId) }
)
Write-Output "dashboard-tui processes after: $($after.Count)"
if ($newLeftovers.Count -gt 0) {
    Write-Output "leftover dashboard process(es):"
    $newLeftovers | Format-Table -AutoSize | Out-String | Write-Output
    Write-Error "Dashboard launcher left child process(es) behind."
    exit 5
}

Write-Output "Dashboard E2E debug passed."

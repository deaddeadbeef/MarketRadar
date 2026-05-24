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

$startInfo = New-Object System.Diagnostics.ProcessStartInfo
$startInfo.FileName = $powerShellExe
$startInfo.WorkingDirectory = [string]$repoRoot
$startInfo.UseShellExecute = $false
$startInfo.RedirectStandardOutput = $true
$startInfo.RedirectStandardError = $true
$startInfo.CreateNoWindow = $true
$startInfo.Arguments = ($arguments | ForEach-Object { ConvertTo-ProcessArgument $_ }) -join " "

$script:DashboardDebugStdout = New-Object System.Text.StringBuilder
$script:DashboardDebugStderr = New-Object System.Text.StringBuilder
$outputHandler = [System.Diagnostics.DataReceivedEventHandler]{
    param($sender, $eventArgs)
    if ($null -ne $eventArgs.Data) {
        [void]$script:DashboardDebugStdout.AppendLine($eventArgs.Data)
    }
}
$errorHandler = [System.Diagnostics.DataReceivedEventHandler]{
    param($sender, $eventArgs)
    if ($null -ne $eventArgs.Data) {
        [void]$script:DashboardDebugStderr.AppendLine($eventArgs.Data)
    }
}

$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$process = New-Object System.Diagnostics.Process
$process.StartInfo = $startInfo
$process.add_OutputDataReceived($outputHandler)
$process.add_ErrorDataReceived($errorHandler)
if (-not $process.Start()) {
    throw "Failed to start launcher process."
}
if ($null -eq $process) {
    throw "Failed to start launcher process."
}
$process.BeginOutputReadLine()
$process.BeginErrorReadLine()

try {
    if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
        Stop-ProcessTree -Process $process
        Write-Error "Dashboard E2E timed out after $TimeoutSeconds seconds."
        exit 2
    }
    $process.WaitForExit()
    $stdout = $script:DashboardDebugStdout.ToString()
    $stderr = $script:DashboardDebugStderr.ToString()
}
finally {
    try {
        $process.remove_OutputDataReceived($outputHandler)
        $process.remove_ErrorDataReceived($errorHandler)
    }
    catch {
        # The process may already be torn down after timeout cleanup.
    }
    Stop-ProcessTree -Process $process
    $script:DashboardDebugStdout = $null
    $script:DashboardDebugStderr = $null
}
$stopwatch.Stop()

Write-Output ("launcher exit code: {0}" -f $process.ExitCode)
Write-Output ("elapsed seconds: {0:n1}" -f $stopwatch.Elapsed.TotalSeconds)
if (-not [string]::IsNullOrWhiteSpace($stderr)) {
    Write-Output "stderr:"
    Write-Output $stderr
}

if ($process.ExitCode -ne 0) {
    Write-Output "stdout:"
    Write-Output $stdout
    exit $process.ExitCode
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

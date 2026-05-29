param(
    [string]$Page = "overview",
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

function Get-DashboardExpectedPageLabels {
    param([string]$Page)

    $normalized = $Page
    if ([string]::IsNullOrWhiteSpace($normalized)) {
        $normalized = "overview"
    }
    $normalized = $normalized.Trim().ToLowerInvariant()
    $normalized = $normalized -replace "[\s_]+", "-"
    if ($normalized.StartsWith("candidate:")) {
        $candidateTicker = $Page.Split(":", 2)[1].Trim().ToUpperInvariant()
        if (-not [string]::IsNullOrWhiteSpace($candidateTicker)) {
            return @("Candidate $candidateTicker")
        }
    }
    switch ($normalized) {
        "0" { @("tutorial", "Start") }
        "learn" { @("tutorial", "Start") }
        "start" { @("tutorial", "Start") }
        "tut" { @("tutorial", "Start") }
        "tutorial" { @("tutorial", "Start") }
        "1" { @("overview", "Inbox") }
        "overview" { @("overview", "Inbox") }
        "home" { @("overview", "Inbox") }
        "inbox" { @("overview", "Inbox") }
        "insight" { @("overview", "Inbox") }
        "insights" { @("overview", "Inbox") }
        "mail" { @("overview", "Inbox") }
        "messages" { @("overview", "Inbox") }
        "o" { @("overview", "Inbox") }
        "2" { @("readiness", "Evidence Gaps") }
        "blockers" { @("readiness", "Evidence Gaps") }
        "evidence" { @("readiness", "Evidence Gaps") }
        "evidence-gaps" { @("readiness", "Evidence Gaps") }
        "gaps" { @("readiness", "Evidence Gaps") }
        "ready" { @("readiness", "Evidence Gaps") }
        "readiness" { @("readiness", "Evidence Gaps") }
        "3" { @("run", "Safe Run") }
        "call-plan" { @("run", "Safe Run") }
        "plan" { @("run", "Safe Run") }
        "safe" { @("run", "Safe Run") }
        "safe-run" { @("run", "Safe Run") }
        "run" { @("run", "Safe Run") }
        "4" { @("candidates", "Candidate Review") }
        "c" { @("candidates", "Candidate Review") }
        "candidate" { @("candidates", "Candidate Review") }
        "candidate-review" { @("candidates", "Candidate Review") }
        "agent" { @("agent", "Agent Coach") }
        "agents" { @("agent", "Agent Coach") }
        "candidates" { @("candidates", "Candidate Review") }
        "11" { @("review", "Decision Review") }
        "d" { @("review", "Decision Review") }
        "decision" { @("review", "Decision Review") }
        "decisions" { @("review", "Decision Review") }
        "decision-ready" { @("review", "Decision Review") }
        "review" { @("review", "Decision Review") }
        "5" { @("alerts", "Alerts") }
        "a" { @("alerts", "Alerts") }
        "alerts" { @("alerts", "Alerts") }
        "6" { @("ipo", "IPO/S-1") }
        "ipo" { @("ipo", "IPO/S-1") }
        "s1" { @("ipo", "IPO/S-1") }
        "7" { @("broker", "Broker") }
        "b" { @("broker", "Broker") }
        "broker" { @("broker", "Broker") }
        "8" { @("ops", "Ops") }
        "ops" { @("ops", "Ops") }
        "9" { @("telemetry", "Telemetry") }
        "t" { @("telemetry", "Telemetry") }
        "telemetry" { @("telemetry", "Telemetry") }
        "10" { @("agent", "Agent Coach") }
        "brief" { @("agent", "Agent Coach") }
        "themes" { @("themes", "Themes") }
        "validation" { @("validation", "Validation") }
        "costs" { @("costs", "Costs") }
        "features" { @("features", "Features") }
        "help" { @("help", "Help") }
        default { @($Page) }
    }
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

$expectedPageLabels = @(Get-DashboardExpectedPageLabels -Page $Page)
$expectedPagePattern = ($expectedPageLabels | ForEach-Object { [regex]::Escape($_) }) -join "|"
if ($stdout -notmatch ("Page:\s*({0})" -f $expectedPagePattern)) {
    Write-Output "stdout:"
    Write-Output $stdout
    Write-Error "Dashboard debug smoke did not open the expected page."
    exit 6
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

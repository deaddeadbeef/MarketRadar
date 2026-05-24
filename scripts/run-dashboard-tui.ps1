param()

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$dashboardExe = Join-Path $repoRoot ".venv\Scripts\catalyst-radar.exe"
$stateDir = Join-Path $repoRoot ".state"
$stampPath = Join-Path $stateDir "dashboard-bootstrap.json"

$noUpdate = $false
$forceInstall = $false
$dashboardArgs = New-Object System.Collections.Generic.List[string]

foreach ($arg in $args) {
    switch ($arg) {
        "--no-update" {
            $noUpdate = $true
            continue
        }
        "--force-install" {
            $forceInstall = $true
            continue
        }
        default {
            $dashboardArgs.Add($arg)
        }
    }
}

function Invoke-Checked {
    param(
        [string]$FilePath,
        [string[]]$Arguments
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $FilePath $($Arguments -join ' ')"
    }
}

function Update-CleanMain {
    if ($noUpdate) {
        return
    }
    if ($null -eq (Get-Command git -ErrorAction SilentlyContinue)) {
        Write-Warning "git was not found; skipping repository update."
        return
    }

    $branch = (& git branch --show-current).Trim()
    if ($branch -ne "main") {
        Write-Warning "Current branch is '$branch', not 'main'; skipping repository update."
        return
    }

    $status = & git status --porcelain
    if (-not [string]::IsNullOrWhiteSpace(($status -join ""))) {
        Write-Warning "Working tree has local changes; skipping repository update."
        return
    }

    Invoke-Checked git @("fetch", "origin", "main")
    $local = (& git rev-parse HEAD).Trim()
    $remote = (& git rev-parse origin/main).Trim()
    if ($local -ne $remote) {
        Invoke-Checked git @("pull", "--ff-only", "origin", "main")
    }
}

function Ensure-Venv {
    if (Test-Path -LiteralPath $venvPython) {
        return
    }

    Write-Output "Creating local Python environment at .venv"
    if ($null -eq (Get-Command py -ErrorAction SilentlyContinue)) {
        throw "Python launcher 'py' was not found. Install Python 3.11+ first."
    }
    Invoke-Checked py @("-3.11", "-m", "venv", ".venv")
}

function Get-InstallStamp {
    if (-not (Test-Path -LiteralPath $stampPath)) {
        return $null
    }
    try {
        return Get-Content -LiteralPath $stampPath -Raw | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Ensure-EditableInstall {
    $pyproject = Join-Path $repoRoot "pyproject.toml"
    $pyprojectHash = (Get-FileHash -LiteralPath $pyproject -Algorithm SHA256).Hash
    $stamp = Get-InstallStamp
    $installedHash = if ($null -ne $stamp) { [string]$stamp.pyproject_hash } else { "" }

    if (
        -not $forceInstall -and
        (Test-Path -LiteralPath $dashboardExe) -and
        $installedHash -eq $pyprojectHash
    ) {
        return
    }

    Write-Output "Installing MarketRadar into .venv"
    Invoke-Checked $venvPython @("-m", "pip", "install", "-e", ".")
    New-Item -ItemType Directory -Force -Path $stateDir | Out-Null
    [ordered]@{
        pyproject_hash = $pyprojectHash
        installed_at = (Get-Date).ToUniversalTime().ToString("o")
    } | ConvertTo-Json | Set-Content -LiteralPath $stampPath -Encoding utf8
}

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
    param(
        [AllowNull()][System.Diagnostics.Process]$Process,
        [int[]]$ExtraProcessIds = @()
    )

    $processIds = New-Object 'System.Collections.Generic.List[int]'
    if ($null -ne $Process) {
        try {
            foreach ($childId in @(Get-ChildProcessIds -ProcessId $Process.Id)) {
                $processIds.Add([int]$childId)
            }
        }
        catch {
            # Best-effort cleanup only; the parent may have exited between polls.
        }
    }
    foreach ($id in $ExtraProcessIds) {
        if ($id -gt 0) {
            $processIds.Add([int]$id)
        }
    }
    foreach ($id in $processIds | Select-Object -Unique) {
        try {
            if ($null -ne (Get-Process -Id $id -ErrorAction SilentlyContinue)) {
                Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
            }
        }
        catch {
            # Continue cleaning any remaining children.
        }
    }
    if ($null -ne $Process) {
        try {
            if (-not $Process.HasExited) {
                Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
                $Process.WaitForExit(5000) | Out-Null
            }
        }
        catch {
            # The process may already be gone.
        }
    }
}

function Invoke-DashboardProcess {
    param(
        [string]$FilePath,
        [string[]]$Arguments
    )

    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $FilePath
    $startInfo.WorkingDirectory = [string]$repoRoot
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $false
    $startInfo.Arguments = ($Arguments | ForEach-Object { ConvertTo-ProcessArgument $_ }) -join " "

    $knownChildIds = @{}
    $stopRequested = $false
    $process = [System.Diagnostics.Process]::Start($startInfo)
    if ($null -eq $process) {
        throw "Failed to start MarketRadar dashboard process."
    }

    $cancelHandler = [System.ConsoleCancelEventHandler]{
        param($sender, $eventArgs)
        $eventArgs.Cancel = $true
        $script:DashboardStopRequested = $true
        Stop-ProcessTree `
            -Process $script:DashboardProcess `
            -ExtraProcessIds @($script:DashboardKnownChildIds.Keys)
    }

    $script:DashboardProcess = $process
    $script:DashboardKnownChildIds = $knownChildIds
    $script:DashboardStopRequested = $false
    [Console]::CancelKeyPress += $cancelHandler
    try {
        while (-not $process.WaitForExit(250)) {
            foreach ($childId in @(Get-ChildProcessIds -ProcessId $process.Id)) {
                $knownChildIds[[int]$childId] = $true
            }
            if ($script:DashboardStopRequested) {
                $stopRequested = $true
                break
            }
        }
        if ($stopRequested) {
            Stop-ProcessTree -Process $process -ExtraProcessIds @($knownChildIds.Keys)
            return 130
        }
        $exitCode = $process.ExitCode
    }
    finally {
        [Console]::CancelKeyPress -= $cancelHandler
        Stop-ProcessTree -Process $process -ExtraProcessIds @($knownChildIds.Keys)
        $script:DashboardProcess = $null
        $script:DashboardKnownChildIds = @{}
        $script:DashboardStopRequested = $false
    }
    return $exitCode
}

Push-Location $repoRoot
try {
    Write-Host "Starting MarketRadar dashboard from $repoRoot"
    Write-Host "The first screen should paint immediately; local data continues loading inside the TUI."

    Update-CleanMain
    Ensure-Venv
    Ensure-EditableInstall

    $exitCode = Invoke-DashboardProcess `
        -FilePath $dashboardExe `
        -Arguments (@("dashboard-tui") + $dashboardArgs.ToArray())
    if ($exitCode -ne 0) {
        throw "MarketRadar dashboard exited with code $exitCode."
    }
}
finally {
    Pop-Location
}

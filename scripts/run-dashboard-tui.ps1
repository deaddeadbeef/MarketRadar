param()

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
$venvDir = Join-Path $repoRoot ".venv"
$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
$dashboardExe = Join-Path $repoRoot ".venv\Scripts\catalyst-radar.exe"
$stateDir = Join-Path $repoRoot ".state"
$stampPath = Join-Path $stateDir "dashboard-bootstrap.json"

$noUpdate = $false
$forceInstall = $false
$repairVenv = $false
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
        "--repair-venv" {
            $repairVenv = $true
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

function Get-PythonInstallHint {
    return @(
        "Install a stable Python 3.11+ first, then run radar again.",
        "Recommended Windows install:",
        "  winget install Python.Python.3.11",
        "After install, open a new PowerShell session and run:",
        "  radar --repair-venv",
        "This only repairs MarketRadar's repo-local .venv."
    ) -join [Environment]::NewLine
}

function Test-PythonLauncherHealthy {
    if ($null -eq (Get-Command py -ErrorAction SilentlyContinue)) {
        return $false
    }
    try {
        $null = & py @(
            "-3.11",
            "-c",
            "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 2)"
        ) 2>&1
        return $LASTEXITCODE -eq 0
    }
    catch {
        return $false
    }
}

function Test-VenvPythonHealthy {
    if (-not (Test-Path -LiteralPath $venvPython)) {
        return $false
    }
    try {
        $null = & $venvPython @(
            "-c",
            "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 2)"
        ) 2>&1
        return $LASTEXITCODE -eq 0
    }
    catch {
        return $false
    }
}

function Get-VenvPythonHome {
    $cfg = Join-Path $venvDir "pyvenv.cfg"
    if (-not (Test-Path -LiteralPath $cfg)) {
        return ""
    }
    $homeLine = Get-Content -LiteralPath $cfg -ErrorAction SilentlyContinue |
        Where-Object { $_ -match "^home\s*=" } |
        Select-Object -First 1
    if ([string]::IsNullOrWhiteSpace($homeLine)) {
        return ""
    }
    return ($homeLine -replace "^home\s*=\s*", "").Trim()
}

function Get-BrokenVenvMessage {
    $pythonHome = Get-VenvPythonHome
    $homeDetail = if ([string]::IsNullOrWhiteSpace($pythonHome)) {
        "Recorded Python home: not found in .venv\pyvenv.cfg"
    }
    else {
        "Recorded Python home: $pythonHome"
    }
    return @(
        "MarketRadar local Python environment is broken.",
        "Venv python: $venvPython",
        $homeDetail,
        "This often happens when .venv was created from the Windows Store Python alias and that install moved or expired.",
        "",
        "Clean fix:",
        "  1. Install a stable Python 3.11+ if needed: winget install Python.Python.3.11",
        "  2. Open a new PowerShell session.",
        "  3. Run: radar --repair-venv",
        "",
        "The repair flag only moves and recreates this repo's local .venv."
    ) -join [Environment]::NewLine
}

function Move-BrokenVenvForRepair {
    if (-not (Test-Path -LiteralPath $venvDir)) {
        return
    }
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $target = Join-Path $repoRoot ".venv.broken-$stamp"
    Move-Item -LiteralPath $venvDir -Destination $target
    Write-Warning "Moved broken .venv to $target"
}

function Get-EditableInstallFailureMessage {
    return @(
        "MarketRadar dependency bootstrap failed.",
        "The dashboard did not start because the repo-local .venv is not fully installed.",
        "If you were offline or package download was blocked, reconnect and run:",
        "  radar --force-install",
        "If Python itself is broken, run:",
        "  radar --repair-venv"
    ) -join [Environment]::NewLine
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
    if (Test-VenvPythonHealthy) {
        return
    }

    if (Test-Path -LiteralPath $venvDir) {
        if (-not $repairVenv) {
            throw (Get-BrokenVenvMessage)
        }
        if (-not (Test-PythonLauncherHealthy)) {
            throw (Get-PythonInstallHint)
        }
        Move-BrokenVenvForRepair
    }

    Write-Output "Creating local Python environment at .venv"
    if (-not (Test-PythonLauncherHealthy)) {
        throw (Get-PythonInstallHint)
    }
    Invoke-Checked py @("-3.11", "-m", "venv", ".venv")
    if (-not (Test-VenvPythonHealthy)) {
        throw (Get-BrokenVenvMessage)
    }
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

    Write-Output "Installing MarketRadar into .venv; first install can take a few minutes."
    try {
        Invoke-Checked $venvPython @(
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-input",
            "--timeout",
            "30",
            "--retries",
            "1",
            "-e",
            "."
        )
    }
    catch {
        throw (Get-EditableInstallFailureMessage)
    }
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
    [System.Console]::add_CancelKeyPress($cancelHandler)
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
        [System.Console]::remove_CancelKeyPress($cancelHandler)
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

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

Push-Location $repoRoot
try {
    Update-CleanMain
    Ensure-Venv
    Ensure-EditableInstall

    & $dashboardExe "dashboard-tui" @dashboardArgs
    if ($LASTEXITCODE -ne 0) {
        throw "MarketRadar dashboard exited with code $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}

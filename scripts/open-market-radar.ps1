#Requires -Version 5.1
<#
.SYNOPSIS
  Open MarketRadar World Events. Idempotent. Uses WMI so the window outlives the agent shell.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$liveRoot = "C:\Users\fpan1\MarketRadar"

function Get-ProductRoot {
    $scriptRepo = Split-Path -Parent $PSScriptRoot
    foreach ($candidate in @($liveRoot, $scriptRepo)) {
        $exe = Join-Path $candidate "target\release\radar-desktop.exe"
        $bat = Join-Path $candidate "Open-MarketRadar.bat"
        if ((Test-Path -LiteralPath $exe) -or (Test-Path -LiteralPath $bat)) {
            return $candidate
        }
    }
    return $liveRoot
}

function Get-OpenDesktop {
    Get-Process -Name "radar-desktop" -ErrorAction SilentlyContinue |
        Where-Object { $_.MainWindowTitle }
}

function Wait-OpenDesktop {
    param([int]$Seconds = 15)
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        $proc = Get-OpenDesktop
        if ($proc) { return @($proc)[0] }
        Start-Sleep -Milliseconds 400
    }
    return $null
}

function Get-LaunchSpec {
    param([string]$Root)
    $bat = Join-Path $Root "Open-MarketRadar.bat"
    $liveExe = Join-Path $Root "target\release\radar-desktop.exe"
    $exe = $null
    $snapshot = $null
    if (Test-Path -LiteralPath $bat) {
        $text = Get-Content -LiteralPath $bat -Raw -ErrorAction Stop
        $exeMatch = [regex]::Match($text, '"([^"]+radar-desktop\.exe)"')
        if ($exeMatch.Success) { $exe = $exeMatch.Groups[1].Value }
        $snapMatch = [regex]::Match($text, '--snapshot-command\s+"([^"]+)"')
        if ($snapMatch.Success) { $snapshot = $snapMatch.Groups[1].Value }
    }
    if (-not $exe -or -not (Test-Path -LiteralPath $exe)) { $exe = $liveExe }
    if (-not (Test-Path -LiteralPath $exe)) { return $null }
    if (-not $snapshot) {
        $python = Join-Path $Root ".venv\Scripts\python.exe"
        if (-not (Test-Path -LiteralPath $python)) { $python = "python" }
        $snapshotPy = Join-Path $Root "scripts\discovery-snapshot.py"
        if (Test-Path -LiteralPath $snapshotPy) {
            $snapshot = "& '$python' '$snapshotPy'"
        }
    }
    return [pscustomobject]@{ Exe = $exe; SnapshotCommand = $snapshot; Root = $Root }
}

$running = Get-OpenDesktop
if ($running) {
    Write-Output "status=already_running action=press_R pid=$(@($running)[0].Id)"
    exit 0
}

$spec = Get-LaunchSpec -Root (Get-ProductRoot)
if (-not $spec) {
    Write-Output "status=error next=missing Open-MarketRadar.bat and target\release\radar-desktop.exe"
    exit 1
}

$command = '"{0}" --page world-events' -f $spec.Exe
if ($spec.SnapshotCommand) {
    $command += ' --snapshot-command "{0}"' -f $spec.SnapshotCommand
}

$created = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
    CommandLine = $command
    CurrentDirectory = $spec.Root
}
if ($created.ReturnValue -ne 0) {
    Write-Output "status=error wmi=$($created.ReturnValue) next=Win32_Process.Create failed"
    exit 1
}

$opened = Wait-OpenDesktop -Seconds 15
if ($opened) {
    Write-Output "status=launched source=$($spec.Exe) page=world-events pid=$($opened.Id)"
    exit 0
}

Write-Output "status=error wmi_pid=$($created.ProcessId) next=radar-desktop did not stay running"
exit 1

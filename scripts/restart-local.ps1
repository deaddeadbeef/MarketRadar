param(
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8443,
    [string]$DashboardHost = "127.0.0.1",
    [int]$DashboardPort = 8514,
    [switch]$SkipHealthCheck
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
$StateDir = Join-Path $RepoRoot ".state\processes"
$PythonPath = Join-Path $RepoRoot "src"
$ApiOut = Join-Path $StateDir "api.out.log"
$ApiErr = Join-Path $StateDir "api.err.log"
$DashboardOut = Join-Path $StateDir "dashboard.out.log"
$DashboardErr = Join-Path $StateDir "dashboard.err.log"
$ApiKeyFile = Join-Path $RepoRoot "data\local\schwab-localhost-key.pem"
$ApiCertFile = Join-Path $RepoRoot "data\local\schwab-localhost-cert.pem"

Set-Location $RepoRoot
New-Item -ItemType Directory -Force -Path $StateDir | Out-Null

$running = Get-CimInstance Win32_Process |
    Where-Object {
        ($_.Name -like "python*") -and
        ($_.CommandLine -match "apps\.api\.main|apps/dashboard/Home\.py|apps\\dashboard\\Home\.py")
    }

foreach ($proc in $running) {
    Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
}

Start-Sleep -Seconds 2

$previousPythonPath = $env:PYTHONPATH
$env:PYTHONPATH = $PythonPath

$apiArgs = @(
    "-m", "uvicorn", "apps.api.main:app",
    "--host", $ApiHost,
    "--port", [string]$ApiPort,
    "--ssl-keyfile", $ApiKeyFile,
    "--ssl-certfile", $ApiCertFile
)
$dashboardArgs = @(
    "-m", "streamlit", "run", "apps/dashboard/Home.py",
    "--server.headless", "true",
    "--server.port", [string]$DashboardPort,
    "--server.address", $DashboardHost
)

$apiProcess = Start-Process -FilePath "python" `
    -ArgumentList $apiArgs `
    -WorkingDirectory $RepoRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $ApiOut `
    -RedirectStandardError $ApiErr `
    -PassThru

$dashboardProcess = Start-Process -FilePath "python" `
    -ArgumentList $dashboardArgs `
    -WorkingDirectory $RepoRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $DashboardOut `
    -RedirectStandardError $DashboardErr `
    -PassThru

if ($null -eq $previousPythonPath) {
    Remove-Item Env:\PYTHONPATH -ErrorAction SilentlyContinue
}
else {
    $env:PYTHONPATH = $previousPythonPath
}

if (-not $SkipHealthCheck) {
    $healthUri = "https://$ApiHost`:$ApiPort/api/health"
    $deadline = (Get-Date).AddSeconds(30)
    $previousCertificateCallback = [System.Net.ServicePointManager]::ServerCertificateValidationCallback
    [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
    try {
        do {
            try {
                $response = Invoke-WebRequest -Uri $healthUri -UseBasicParsing -TimeoutSec 5
                if ($response.StatusCode -eq 200) {
                    $apiHealthy = $true
                    break
                }
            }
            catch {
                Start-Sleep -Seconds 1
            }
        } while ((Get-Date) -lt $deadline)
    }
    finally {
        [System.Net.ServicePointManager]::ServerCertificateValidationCallback = $previousCertificateCallback
    }

    if (-not $apiHealthy) {
        throw "API did not become healthy within 30 seconds. See $ApiErr"
    }
}

[PSCustomObject]@{
    ApiPid = $apiProcess.Id
    DashboardPid = $dashboardProcess.Id
    ApiUrl = "https://$ApiHost`:$ApiPort"
    DashboardUrl = "http://$DashboardHost`:$DashboardPort"
    ApiLog = $ApiErr
    DashboardLog = $DashboardErr
}

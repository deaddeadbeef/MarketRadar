param(
    [string]$AsOf = "",
    [int]$TickerPages = 0,
    [double]$TickerPageDelaySeconds = -1,
    [switch]$AllowPartial,
    [switch]$Execute
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
$dashboardExe = Join-Path $repoRoot ".venv\Scripts\catalyst-radar.exe"
$hadTickerMaxPages = Test-Path Env:CATALYST_POLYGON_TICKERS_MAX_PAGES
$previousTickerMaxPages = $env:CATALYST_POLYGON_TICKERS_MAX_PAGES
$hadTickerPageDelay = Test-Path Env:CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS
$previousTickerPageDelay = $env:CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS

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

function Invoke-RadarJson {
    param([string[]]$Arguments)

    $output = & $dashboardExe @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: catalyst-radar $($Arguments -join ' ')"
    }
    try {
        return $output | ConvertFrom-Json
    }
    catch {
        throw "Command returned invalid JSON: catalyst-radar $($Arguments -join ' ')"
    }
}

Push-Location $repoRoot
try {
    if (-not (Test-Path -LiteralPath $dashboardExe)) {
        throw "Missing .venv catalyst-radar executable. Run scripts\run-dashboard-tui.ps1 once to bootstrap the local install."
    }

    $preflight = Invoke-RadarJson @("priced-in-preflight", "--json")
    $provider = $preflight.provider
    $estimatedPages = [int]$provider.estimated_ticker_seed_pages
    $configuredPages = [int]$provider.ticker_seed_cap_pages
    $resolvedPages = if ($TickerPages -gt 0) { $TickerPages } elseif ($estimatedPages -gt 0) { $estimatedPages } else { $configuredPages }
    $resolvedDelay = if ($TickerPageDelaySeconds -ge 0) { $TickerPageDelaySeconds } else { [double]$provider.ticker_page_delay_seconds }
    $resolvedAsOf = if (-not [string]::IsNullOrWhiteSpace($AsOf)) { $AsOf } else { [string]$provider.latest_daily_bar_date }
    if ([string]::IsNullOrWhiteSpace($resolvedAsOf)) {
        throw "No AsOf date was provided and preflight has no latest daily-bar date."
    }

    Write-Output ("Preflight: status={0}; scan_status={1}; external_calls={2}" -f $preflight.status, $preflight.scan_status, $preflight.external_calls_made)
    Write-Output ("Market provider: {0}" -f $provider.market_provider)
    Write-Output ("Latest bars: date={0}; tickers={1}" -f $provider.latest_daily_bar_date, $provider.latest_daily_bar_ticker_count)
    Write-Output ("Ticker seed pages: configured={0}; estimated={1}; selected={2}" -f $configuredPages, $estimatedPages, $resolvedPages)
    Write-Output ("Ticker page delay seconds: {0}" -f $resolvedDelay)
    Write-Output ("Scan as-of: {0}" -f $resolvedAsOf)

    if ($provider.market_provider -ne "polygon") {
        Write-Output "External calls made: 0"
        throw "Full-market live scan script currently requires CATALYST_DAILY_MARKET_PROVIDER=polygon."
    }

    if ($estimatedPages -gt 0 -and $resolvedPages -lt $estimatedPages -and -not $AllowPartial) {
        Write-Output "External calls made: 0"
        throw "Selected TickerPages=$resolvedPages is below estimated full seed pages=$estimatedPages. Re-run with -TickerPages $estimatedPages or -AllowPartial."
    }

    Write-Output ""
    if (-not $Execute) {
        Write-Output "Plan only: no provider calls or database writes were made."
        Write-Output "Re-run with -Execute to run this sequence in the current PowerShell process:"
        Write-Output ('$env:CATALYST_POLYGON_TICKERS_MAX_PAGES="{0}"' -f $resolvedPages)
        Write-Output ('$env:CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS="{0}"' -f $resolvedDelay)
        Write-Output ("catalyst-radar ingest-polygon tickers --max-pages {0}" -f $resolvedPages)
        Write-Output ("catalyst-radar run-daily --as-of {0} --available-at <UTC-now> --json" -f $resolvedAsOf)
        Write-Output "catalyst-radar priced-in-queue --json"
        Write-Output "External calls made: 0"
        return
    }

    $env:CATALYST_POLYGON_TICKERS_MAX_PAGES = [string]$resolvedPages
    $env:CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS = [string]$resolvedDelay
    $availableAt = [DateTimeOffset]::UtcNow.ToString("o")

    Write-Output "Executing full-market scheduled scan. Provider calls are explicit below."
    Invoke-Checked $dashboardExe @("ingest-polygon", "tickers", "--max-pages", [string]$resolvedPages)
    Invoke-Checked $dashboardExe @("run-daily", "--as-of", $resolvedAsOf, "--available-at", $availableAt, "--json")
    Invoke-Checked $dashboardExe @("priced-in-queue", "--json")
    Write-Output ("External provider call budget requested: polygon_ticker_pages={0}; grouped_daily=1" -f $resolvedPages)
}
finally {
    if ($hadTickerMaxPages) {
        $env:CATALYST_POLYGON_TICKERS_MAX_PAGES = $previousTickerMaxPages
    }
    else {
        Remove-Item Env:CATALYST_POLYGON_TICKERS_MAX_PAGES -ErrorAction SilentlyContinue
    }
    if ($hadTickerPageDelay) {
        $env:CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS = $previousTickerPageDelay
    }
    else {
        Remove-Item Env:CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS -ErrorAction SilentlyContinue
    }
    Pop-Location
}

param(
    [string]$AsOf = "",
    [int]$TickerPages = 0,
    [double]$TickerPageDelaySeconds = -1,
    [string]$UniverseName = "",
    [switch]$UseUniverse,
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
$hadDailyMarketProvider = Test-Path Env:CATALYST_DAILY_MARKET_PROVIDER
$previousDailyMarketProvider = $env:CATALYST_DAILY_MARKET_PROVIDER

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

function Invoke-DailyRunAcceptCompletedScan {
    param([string[]]$Arguments)

    $output = & $dashboardExe @Arguments
    $exitCode = $LASTEXITCODE
    try {
        $payload = $output | ConvertFrom-Json
    }
    catch {
        throw "Command returned invalid JSON: catalyst-radar $($Arguments -join ' ')"
    }

    $featureScan = $payload.daily_result.steps.feature_scan
    if ($exitCode -ne 0 -and $featureScan.status -ne "success") {
        throw "Daily run failed before completing feature_scan: catalyst-radar $($Arguments -join ' ')"
    }
    if ($exitCode -ne 0) {
        Write-Output ("Daily run completed feature_scan={0} rows, but returned status={1}; continuing to queue review." -f $featureScan.normalized_count, $payload.daily_result.status)
    }
    return $payload
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
    $preflightTargetAsOf = [string]$preflight.target_as_of
    $latestDailyBarDate = [string]$provider.latest_daily_bar_date
    if (-not [string]::IsNullOrWhiteSpace($AsOf)) {
        $resolvedAsOf = $AsOf
        $resolvedAsOfSource = "argument"
    }
    elseif (-not [string]::IsNullOrWhiteSpace($preflightTargetAsOf)) {
        $resolvedAsOf = $preflightTargetAsOf
        $resolvedAsOfSource = [string]$preflight.target_as_of_source
    }
    else {
        $resolvedAsOf = $latestDailyBarDate
        $resolvedAsOfSource = "latest_daily_bar"
    }
    $marketProvider = [string]$provider.market_provider
    $resolvedUniverseName = if (-not [string]::IsNullOrWhiteSpace($UniverseName)) {
        $UniverseName
    }
    elseif (-not [string]::IsNullOrWhiteSpace($env:CATALYST_UNIVERSE_NAME)) {
        $env:CATALYST_UNIVERSE_NAME
    }
    else {
        "liquid-us"
    }
    if ([string]::IsNullOrWhiteSpace($resolvedAsOf)) {
        throw "No AsOf date was provided and preflight has no target or latest daily-bar date."
    }

    Write-Output ("Preflight: status={0}; scan_status={1}; external_calls={2}" -f $preflight.status, $preflight.scan_status, $preflight.external_calls_made)
    Write-Output ("Market provider: {0}" -f $marketProvider)
    if ($UseUniverse) {
        Write-Output ("Scan scope: selected universe {0}" -f $resolvedUniverseName)
    }
    else {
        Write-Output "Scan scope: all active securities with available bars"
    }
    Write-Output ("Latest bars: date={0}; tickers={1}" -f $provider.latest_daily_bar_date, $provider.latest_daily_bar_ticker_count)
    Write-Output ("Ticker seed pages: configured={0}; estimated={1}; selected={2}" -f $configuredPages, $estimatedPages, $resolvedPages)
    Write-Output ("Ticker page delay seconds: {0}" -f $resolvedDelay)
    Write-Output ("Scan as-of: {0}; source={1}" -f $resolvedAsOf, $resolvedAsOfSource)

    if ($marketProvider -ne "polygon") {
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
        Write-Output "Add -UseUniverse only if you intentionally want a selected liquid universe instead of all active securities."
        Write-Output ('$env:CATALYST_POLYGON_TICKERS_MAX_PAGES="{0}"' -f $resolvedPages)
        Write-Output ('$env:CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS="{0}"' -f $resolvedDelay)
        Write-Output ("catalyst-radar ingest-polygon tickers --max-pages {0}" -f $resolvedPages)
        Write-Output ("catalyst-radar ingest-polygon grouped-daily --date {0}" -f $resolvedAsOf)
        Write-Output '$env:CATALYST_DAILY_MARKET_PROVIDER="off"'
        if ($UseUniverse) {
            Write-Output ("catalyst-radar build-universe --as-of {0} --available-at <UTC-now> --name {1} --provider polygon" -f $resolvedAsOf, $resolvedUniverseName)
            Write-Output ("catalyst-radar run-daily --as-of {0} --available-at <UTC-now> --provider polygon --universe {1} --json" -f $resolvedAsOf, $resolvedUniverseName)
        }
        else {
            Write-Output ("catalyst-radar run-daily --as-of {0} --available-at <UTC-now> --provider polygon --json" -f $resolvedAsOf)
        }
        Write-Output "catalyst-radar priced-in-queue --json"
        Write-Output "External calls made: 0"
        return
    }

    $env:CATALYST_POLYGON_TICKERS_MAX_PAGES = [string]$resolvedPages
    $env:CATALYST_POLYGON_TICKER_PAGE_DELAY_SECONDS = [string]$resolvedDelay

    Write-Output "Executing full-market scheduled scan. Provider calls are explicit below."
    Invoke-Checked $dashboardExe @("ingest-polygon", "tickers", "--max-pages", [string]$resolvedPages)
    Invoke-Checked $dashboardExe @("ingest-polygon", "grouped-daily", "--date", $resolvedAsOf)
    $availableAt = [DateTimeOffset]::UtcNow.ToString("o")
    $env:CATALYST_DAILY_MARKET_PROVIDER = "off"
    if ($UseUniverse) {
        Invoke-Checked $dashboardExe @("build-universe", "--as-of", $resolvedAsOf, "--available-at", $availableAt, "--name", $resolvedUniverseName, "--provider", "polygon")
        Invoke-DailyRunAcceptCompletedScan @("run-daily", "--as-of", $resolvedAsOf, "--available-at", $availableAt, "--provider", "polygon", "--universe", $resolvedUniverseName, "--json") | Out-Null
    }
    else {
        Invoke-DailyRunAcceptCompletedScan @("run-daily", "--as-of", $resolvedAsOf, "--available-at", $availableAt, "--provider", "polygon", "--json") | Out-Null
    }
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
    if ($hadDailyMarketProvider) {
        $env:CATALYST_DAILY_MARKET_PROVIDER = $previousDailyMarketProvider
    }
    else {
        Remove-Item Env:CATALYST_DAILY_MARKET_PROVIDER -ErrorAction SilentlyContinue
    }
    Pop-Location
}

param(
    [string]$Securities = "data/sample/securities.csv",
    [string]$DailyBars = "data/sample/daily_bars.csv",
    [string]$Holdings = "data/sample/holdings.csv",
    [string]$ExpectedAsOf,
    [switch]$Execute
)

$ErrorActionPreference = "Stop"

function Resolve-InputPath {
    param(
        [string]$Path,
        [string]$Label,
        [switch]$Optional
    )

    if ([string]::IsNullOrWhiteSpace($Path)) {
        if ($Optional) {
            return $null
        }
        throw "$Label path is required."
    }

    $resolved = @(Resolve-Path -LiteralPath $Path -ErrorAction SilentlyContinue)
    if ($resolved.Count -eq 0) {
        if ($Optional) {
            return $null
        }
        throw "$Label file not found: $Path"
    }
    return $resolved[0].ProviderPath
}

function Convert-CsvDate {
    param(
        [object]$Value,
        [string]$Field
    )

    try {
        return [datetime]::Parse(
            [string]$Value,
            [System.Globalization.CultureInfo]::InvariantCulture
        ).Date
    }
    catch {
        throw "Invalid $Field date value: $Value"
    }
}

function Assert-Columns {
    param(
        [object]$Row,
        [string[]]$Required,
        [string]$Label
    )

    $columns = @($Row.PSObject.Properties.Name)
    foreach ($column in $Required) {
        if ($columns -notcontains $column) {
            throw "$Label is missing required column: $column"
        }
    }
}

$resolvedSecurities = Resolve-InputPath -Path $Securities -Label "Securities"
$resolvedDailyBars = Resolve-InputPath -Path $DailyBars -Label "Daily bars"
$resolvedHoldings = Resolve-InputPath -Path $Holdings -Label "Holdings" -Optional

$barRows = @(Import-Csv -LiteralPath $resolvedDailyBars)
if ($barRows.Count -eq 0) {
    throw "Daily bars CSV contains no rows: $DailyBars"
}
Assert-Columns -Row $barRows[0] -Label "Daily bars CSV" -Required @(
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "adjusted",
    "provider",
    "source_ts",
    "available_at"
)

$latestBarDate = $null
foreach ($row in $barRows) {
    $barDate = Convert-CsvDate -Value $row.date -Field "daily bar"
    if ($null -eq $latestBarDate -or $barDate -gt $latestBarDate) {
        $latestBarDate = $barDate
    }
}
$tickers = @(
    $barRows |
        ForEach-Object { ([string]$_.ticker).Trim().ToUpperInvariant() } |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Sort-Object -Unique
)

$expectedDate = $null
if (-not [string]::IsNullOrWhiteSpace($ExpectedAsOf)) {
    $expectedDate = Convert-CsvDate -Value $ExpectedAsOf -Field "expected as-of"
}

$freshnessStatus = "not_checked"
if ($null -ne $expectedDate) {
    $freshnessStatus = if ($latestBarDate -lt $expectedDate) { "stale" } else { "fresh_enough" }
}

Write-Output "CSV market data refresh"
Write-Output ("Daily bars: {0}" -f $resolvedDailyBars)
Write-Output ("Rows: {0}; tickers={1}; latest_bar={2}" -f $barRows.Count, $tickers.Count, $latestBarDate.ToString("yyyy-MM-dd"))
if ($null -ne $expectedDate) {
    Write-Output ("Freshness check: {0}; expected_as_of={1}" -f $freshnessStatus, $expectedDate.ToString("yyyy-MM-dd"))
}
if ($null -eq $resolvedHoldings) {
    Write-Output "Holdings: skipped; optional holdings file was not found."
}
else {
    Write-Output ("Holdings: {0}" -f $resolvedHoldings)
}

if ($freshnessStatus -eq "stale") {
    Write-Output "External calls made: 0"
    Write-Output "Refusing to import stale bars for the requested as-of date."
    Write-Output "Provide a CSV whose latest date is at least the ExpectedAsOf date, or omit -ExpectedAsOf for an explicit manual import."
    exit 2
}

if (-not $Execute) {
    Write-Output ""
    Write-Output "Plan only: no database writes were made."
    Write-Output "Re-run with -Execute to import these CSV rows into the local MarketRadar database."
    Write-Output "External calls made: 0"
    return
}

$cliArgs = @(
    "-m",
    "catalyst_radar.cli",
    "ingest-csv",
    "--securities",
    $resolvedSecurities,
    "--daily-bars",
    $resolvedDailyBars
)
if ($null -ne $resolvedHoldings) {
    $cliArgs += @("--holdings", $resolvedHoldings)
}

$previousPythonPath = $env:PYTHONPATH
$srcPath = Join-Path (Get-Location) "src"
try {
    if ([string]::IsNullOrWhiteSpace($previousPythonPath)) {
        $env:PYTHONPATH = $srcPath
    }
    else {
        $env:PYTHONPATH = "$srcPath;$previousPythonPath"
    }

    & py @cliArgs
    $exitCode = $LASTEXITCODE
}
finally {
    $env:PYTHONPATH = $previousPythonPath
}

if ($exitCode -ne 0) {
    Write-Output "External calls made: 0"
    exit $exitCode
}

Write-Output "CSV market refresh imported."
Write-Output "External calls made: 0"
Write-Output "Next: powershell -ExecutionPolicy Bypass -File scripts\market-radar-status.ps1"
Write-Output "Then run the plan-only smoke before any capped live cycle: powershell -ExecutionPolicy Bypass -File scripts\run-first-live-smoke.ps1"

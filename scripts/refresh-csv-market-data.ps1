param(
    [string]$Securities = "data/sample/securities.csv",
    [string]$DailyBars = "data/sample/daily_bars.csv",
    [string]$Holdings = "data/sample/holdings.csv",
    [string]$ExpectedAsOf,
    [string]$TemplateOut,
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

function Convert-CsvBool {
    param(
        [object]$Value,
        [string]$Field
    )

    $normalized = ([string]$Value).Trim().ToLowerInvariant()
    if ($normalized -in @("1", "true", "yes", "on")) {
        return $true
    }
    if ($normalized -in @("0", "false", "no", "off")) {
        return $false
    }
    throw "Invalid $Field boolean value: $Value"
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

function Assert-NumberField {
    param(
        [object]$Row,
        [string]$Field,
        [string]$Label
    )

    $value = [string]$Row.PSObject.Properties[$Field].Value
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "$Label is missing required numeric field: $Field"
    }
    try {
        [double]::Parse(
            $value,
            [System.Globalization.NumberStyles]::Float,
            [System.Globalization.CultureInfo]::InvariantCulture
        ) | Out-Null
    }
    catch {
        throw "$Label has invalid numeric field $Field`: $value"
    }
}

function Assert-IntegerField {
    param(
        [object]$Row,
        [string]$Field,
        [string]$Label
    )

    $value = [string]$Row.PSObject.Properties[$Field].Value
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "$Label is missing required integer field: $Field"
    }
    try {
        [int64]::Parse(
            $value,
            [System.Globalization.NumberStyles]::Integer,
            [System.Globalization.CultureInfo]::InvariantCulture
        ) | Out-Null
    }
    catch {
        throw "$Label has invalid integer field $Field`: $value"
    }
}

function Assert-DailyBarRows {
    param([object[]]$Rows)

    $issues = [System.Collections.Generic.List[string]]::new()
    $rowNumber = 1
    foreach ($row in $Rows) {
        $rowNumber += 1
        $ticker = ([string]$row.ticker).Trim().ToUpperInvariant()
        $dateLabel = ([string]$row.date).Trim()
        $label = if ([string]::IsNullOrWhiteSpace($ticker)) {
            "Daily bar CSV row $rowNumber"
        }
        else {
            "Daily bar row $ticker $dateLabel"
        }
        if ([string]::IsNullOrWhiteSpace($ticker)) {
            $issues.Add("$label is missing required ticker.")
        }
        try {
            Convert-CsvDate -Value $row.date -Field "daily bar" | Out-Null
        }
        catch {
            $issues.Add($_.Exception.Message)
        }
        foreach ($field in @("open", "high", "low", "close")) {
            try {
                Assert-NumberField -Row $row -Field $field -Label $label
            }
            catch {
                $issues.Add($_.Exception.Message)
            }
        }
        try {
            Assert-IntegerField -Row $row -Field "volume" -Label $label
        }
        catch {
            $issues.Add($_.Exception.Message)
        }
        try {
            Assert-NumberField -Row $row -Field "vwap" -Label $label
        }
        catch {
            $issues.Add($_.Exception.Message)
        }
        try {
            Convert-CsvBool -Value $row.adjusted -Field "adjusted" | Out-Null
        }
        catch {
            $issues.Add($_.Exception.Message)
        }
        if ([string]::IsNullOrWhiteSpace(([string]$row.provider))) {
            $issues.Add("$label is missing required provider.")
        }
        try {
            [datetime]::Parse(
                [string]$row.source_ts,
                [System.Globalization.CultureInfo]::InvariantCulture
            ) | Out-Null
        }
        catch {
            $issues.Add("$label has invalid source_ts: $($row.source_ts)")
        }
        try {
            [datetime]::Parse(
                [string]$row.available_at,
                [System.Globalization.CultureInfo]::InvariantCulture
            ) | Out-Null
        }
        catch {
            $issues.Add("$label has invalid available_at: $($row.available_at)")
        }
    }

    if ($issues.Count -gt 0) {
        $issueLimit = 60
        Write-Output "Daily bars CSV validation failed: $($issues.Count) issue(s)."
        foreach ($issue in ($issues | Select-Object -First $issueLimit)) {
            Write-Output "- $issue"
        }
        if ($issues.Count -gt $issueLimit) {
            Write-Output "- plus $($issues.Count - $issueLimit) more issue(s)"
        }
        Write-Output "Fix the rows above, then preview again before importing."
        throw "Daily bars CSV validation failed."
    }
}

function Get-ActiveSecurityTickers {
    param([string]$Path)

    $securityRows = @(Import-Csv -LiteralPath $Path)
    if ($securityRows.Count -eq 0) {
        throw "Securities CSV contains no rows: $Path"
    }
    Assert-Columns -Row $securityRows[0] -Label "Securities CSV" -Required @(
        "ticker",
        "is_active"
    )

    $tickers = @(
        $securityRows |
            Where-Object { Convert-CsvBool -Value $_.is_active -Field "is_active" } |
            ForEach-Object { ([string]$_.ticker).Trim().ToUpperInvariant() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Sort-Object -Unique
    )
    if ($tickers.Count -eq 0) {
        throw "No active tickers found in securities CSV: $Path"
    }
    return $tickers
}

function Write-DailyBarTemplate {
    param(
        [string]$Path,
        [string]$SecuritiesPath,
        [datetime]$AsOfDate
    )

    $tickers = @(Get-ActiveSecurityTickers -Path $SecuritiesPath)

    $parent = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($parent) -and -not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Path $parent | Out-Null
    }

    $asOf = $AsOfDate.ToString("yyyy-MM-dd")
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $lines = @("ticker,date,open,high,low,close,volume,vwap,adjusted,provider,source_ts,available_at")
    foreach ($ticker in $tickers) {
        $lines += (@($ticker, $asOf, "", "", "", "", "", "", "true", "manual_csv", $stamp, $stamp) -join ",")
    }
    Set-Content -LiteralPath $Path -Value $lines -Encoding UTF8

    return $tickers.Count
}

$expectedDate = $null
if (-not [string]::IsNullOrWhiteSpace($ExpectedAsOf)) {
    $expectedDate = Convert-CsvDate -Value $ExpectedAsOf -Field "expected as-of"
}

$resolvedSecurities = Resolve-InputPath -Path $Securities -Label "Securities"
$activeTickers = @(Get-ActiveSecurityTickers -Path $resolvedSecurities)

if (-not [string]::IsNullOrWhiteSpace($TemplateOut)) {
    if ($null -eq $expectedDate) {
        throw "ExpectedAsOf is required when TemplateOut is set."
    }

    $templateRows = Write-DailyBarTemplate `
        -Path $TemplateOut `
        -SecuritiesPath $resolvedSecurities `
        -AsOfDate $expectedDate
    $resolvedTemplate = Resolve-Path -LiteralPath $TemplateOut
    Write-Output "CSV market data template"
    Write-Output ("Template: {0}" -f $resolvedTemplate.ProviderPath)
    Write-Output ("Rows: {0}; expected_as_of={1}" -f $templateRows, $expectedDate.ToString("yyyy-MM-dd"))
    Write-Output "Fill open, high, low, close, volume, and vwap before importing."
    Write-Output (
        "Import command: powershell -ExecutionPolicy Bypass -File scripts\refresh-csv-market-data.ps1 -DailyBars {0} -ExpectedAsOf {1} -Execute" -f
        $TemplateOut,
        $expectedDate.ToString("yyyy-MM-dd")
    )
    Write-Output "External calls made: 0"
    return
}

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
try {
    Assert-DailyBarRows -Rows $barRows
}
catch {
    Write-Output "External calls made: 0"
    exit 2
}

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

$freshnessStatus = "not_checked"
$expectedCoverage = $null
$missingExpectedTickers = @()
if ($null -ne $expectedDate) {
    $freshnessStatus = if ($latestBarDate -lt $expectedDate) { "stale" } else { "fresh_enough" }
    $barTickersAtExpected = @(
        $barRows |
            Where-Object { (Convert-CsvDate -Value $_.date -Field "daily bar") -eq $expectedDate } |
            ForEach-Object { ([string]$_.ticker).Trim().ToUpperInvariant() } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Sort-Object -Unique
    )
    $barTickerSet = @{}
    foreach ($ticker in $barTickersAtExpected) {
        $barTickerSet[$ticker] = $true
    }
    $missingExpectedTickers = @(
        $activeTickers | Where-Object { -not $barTickerSet.ContainsKey($_) }
    )
    $expectedCoverage = [ordered]@{
        Active = $activeTickers.Count
        WithBars = $barTickersAtExpected.Count
        Missing = $missingExpectedTickers.Count
    }
}

Write-Output "CSV market data refresh"
Write-Output ("Daily bars: {0}" -f $resolvedDailyBars)
Write-Output ("Rows: {0}; tickers={1}; latest_bar={2}" -f $barRows.Count, $tickers.Count, $latestBarDate.ToString("yyyy-MM-dd"))
if ($null -ne $expectedDate) {
    Write-Output ("Freshness check: {0}; expected_as_of={1}" -f $freshnessStatus, $expectedDate.ToString("yyyy-MM-dd"))
    Write-Output (
        "Coverage check: active={0}; bars_at_expected_as_of={1}; missing={2}" -f
        $expectedCoverage.Active,
        $expectedCoverage.WithBars,
        $expectedCoverage.Missing
    )
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
if ($missingExpectedTickers.Count -gt 0) {
    $sampleMissing = ($missingExpectedTickers | Select-Object -First 12) -join ", "
    $suffix = if ($missingExpectedTickers.Count -gt 12) { " plus $($missingExpectedTickers.Count - 12) more" } else { "" }
    Write-Output "External calls made: 0"
    Write-Output "Refusing to import incomplete bars for the requested as-of date."
    Write-Output ("Missing expected-as-of bars for active tickers: {0}{1}" -f $sampleMissing, $suffix)
    Write-Output "Generate a template with -TemplateOut, fill every active ticker row, then preview again."
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

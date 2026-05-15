param(
    [string]$OutputPath = "docs\changes\pr-ledger.json",
    [int]$Limit = 200,
    [switch]$Print
)

$ErrorActionPreference = "Stop"
$resolvedLimit = [Math]::Max(1, $Limit)

$raw = & gh pr list `
    --state all `
    --limit $resolvedLimit `
    --json number,title,state,mergedAt,headRefName,baseRefName,url,mergeCommit
if ($LASTEXITCODE -ne 0) {
    throw "Could not read pull request metadata with gh. Authenticate with gh auth login, then retry."
}

try {
    $parsed = ($raw -join "`n") | ConvertFrom-Json
    $pullRequests = @()
    foreach ($item in $parsed) {
        $pullRequests += $item
    }
}
catch {
    throw "gh returned invalid pull request JSON."
}

$mergedPullRequests = @(
    $pullRequests |
        Where-Object { $_.state -eq "MERGED" } |
        Sort-Object number
)

$entries = @()
foreach ($pr in $mergedPullRequests) {
    $mergeCommit = $null
    if ($null -ne $pr.mergeCommit -and $null -ne $pr.mergeCommit.oid) {
        $mergeCommit = [string]$pr.mergeCommit.oid
    }
    $entries += [ordered]@{
        number = [int]$pr.number
        title = [string]$pr.title
        state = [string]$pr.state
        merged_at = [string]$pr.mergedAt
        branch = [string]$pr.headRefName
        base = [string]$pr.baseRefName
        merge_commit = $mergeCommit
        url = [string]$pr.url
    }
}

$latest = $null
if ($entries.Count -gt 0) {
    $latest = $entries[$entries.Count - 1]
}

$remote = ""
try {
    $remote = (& git config --get remote.origin.url).Trim()
}
catch {
    $remote = ""
}

$payload = [ordered]@{
    schema_version = "pr-change-ledger-v1"
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    status = "tracked"
    source = "gh pr list --state all"
    repository_remote = $remote
    gh_limit = $resolvedLimit
    total_prs_seen = $pullRequests.Count
    total_merged = $entries.Count
    latest_merged_pr = $latest
    github_metadata_calls_made = 1
    market_data_broker_llm_calls_made = 0
    entries = $entries
}

$json = $payload | ConvertTo-Json -Depth 20
$parent = Split-Path -Parent $OutputPath
if ([string]::IsNullOrWhiteSpace($parent) -eq $false) {
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
}
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($OutputPath, $json, $utf8NoBom)

Write-Output ("PR change ledger: {0}" -f $OutputPath)
Write-Output (
    "Merged PRs tracked: {0}; latest={1}; GitHub metadata calls: 1" -f
    $entries.Count,
    $(if ($null -ne $latest) { "#{0}" -f $latest.number } else { "n/a" })
)
Write-Output "Market-data/broker/LLM calls made: 0"

if ($Print) {
    Write-Output $json
}

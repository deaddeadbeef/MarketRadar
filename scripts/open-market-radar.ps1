#Requires -Version 5.1
<#
.SYNOPSIS
  DEPRECATED. MarketRadar is a headless CLI. Does not launch the desktop GUI.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
Write-Error @"
DEPRECATED: MarketRadar is a headless CLI. Do not open the GUI.

Use:
  grok -p "/market-radar hunt"
  catalyst-radar convert --posts data/local/inbox/x_posts_YYYY-MM-DD.json --execute
  catalyst-radar brief
  catalyst-radar ready
  catalyst-radar product-scope
"@

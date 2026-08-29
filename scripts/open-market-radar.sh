#!/usr/bin/env bash
# DEPRECATED: Tauri World Events is not the product receive surface.
# MarketRadar is a headless CLI. Do not open the GUI.
set -euo pipefail

echo "DEPRECATED: MarketRadar is a headless CLI. Do not open the GUI." >&2
echo 'Use: grok -p "/market-radar hunt"' >&2
echo "     catalyst-radar convert --posts data/local/inbox/x_posts_YYYY-MM-DD.json --execute" >&2
echo "     catalyst-radar brief" >&2
echo "     catalyst-radar ready" >&2
echo "     catalyst-radar product-scope" >&2
exit 2

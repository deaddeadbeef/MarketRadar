# Task: Real-data discovery insights

## Goal
Make MarketRadar produce a research digest from live X events and live
mapped-ticker Polygon bars.

## Acceptance
1. `discovery-insights` prints a ranked research digest.
2. `discovery-bars --polygon` fetches only event tickers + SPY, with 429 retry.
3. Live 2026-08-13 run: 3 real events, 14 joined names, `assert-discovery-ready` ready=true.
4. Tests cover insights formatting and polygon fixture parse.

## Status
done

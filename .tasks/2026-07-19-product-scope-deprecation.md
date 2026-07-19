# Task: Limit MarketRadar to event-first discovery scope

## Goal
Scope the product to the session’s event-first discovery loop; mark other surfaces deprecated and plan phased removal.

## Acceptance criteria
1. `docs/PRODUCT_SCOPE.md` and `docs/DEPRECATION.md` exist and define in/out of scope.
2. `catalyst-radar product-scope --json` lists active vs deprecated packages/pages/CLI.
3. Deprecated CLI commands emit a DEPRECATED warning on stderr.
4. Desktop page labels mark non-discovery pages as Legacy.
5. Discovery-home nav is World Events + Help only.
6. No large code deletions in D1 (label only).

## Validation
```powershell
pytest tests/unit/test_product_scope.py -q
catalyst-radar product-scope
```

## Status
done — D1 shipped; D2–D5 remain planned in docs/DEPRECATION.md

# Legacy workbench (not the product)

Everything in this folder, plus `handoff.md`, Streamlit (`apps/dashboard/Home.py`),
the Python TUI (`src/catalyst_radar/dashboard/tui.py`), Tauri
(`apps/radar-desktop`), radar-tui (`crates/radar-tui`), and the old
`assert-trial-ready` / `assert-shadow-ready` / `assert-investable-readiness`
gates, describe the **deprecated trading workbench / GUI**.

Current product: headless CLI. See `docs/PRODUCT_SCOPE.md`.

To run a legacy command:

```powershell
$env:CATALYST_ENABLE_LEGACY_WORKBENCH='true'
catalyst-radar dashboard-tui
```

Do not add features here. Do not tell anyone to open the GUI.

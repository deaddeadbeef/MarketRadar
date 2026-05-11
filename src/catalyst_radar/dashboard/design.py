from __future__ import annotations

DASHBOARD_STYLE = """
<style>
:root {
  --mr-primary: #191C1F;
  --mr-secondary: #5C6670;
  --mr-tertiary: #0B7A53;
  --mr-bg: #F7F8FA;
  --mr-surface: #FFFFFF;
  --mr-surface-muted: #EEF2F5;
  --mr-border: #D9DEE5;
  --mr-data-blue: #2563EB;
  --mr-positive: #0B7A53;
  --mr-warning: #A16207;
  --mr-danger: #B42318;
  --mr-font: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
    "Segoe UI", sans-serif;
  --mr-font-data: "IBM Plex Mono", ui-monospace, SFMono-Regular, Consolas, monospace;
}

.stApp {
  background: var(--mr-bg);
  color: var(--mr-primary);
  font-family: var(--mr-font);
}

.block-container {
  max-width: 1440px;
  padding-top: 1.35rem;
  padding-bottom: 2rem;
}

h1 {
  color: var(--mr-primary);
  font-size: 2rem !important;
  font-weight: 650 !important;
  line-height: 1.12 !important;
  letter-spacing: 0 !important;
  margin-bottom: 0.45rem !important;
}

h2, h3 {
  color: var(--mr-primary);
  letter-spacing: 0 !important;
}

h3 {
  font-size: 1.03rem !important;
  font-weight: 640 !important;
  margin-top: 1rem !important;
}

p, label, [data-testid="stMarkdownContainer"] {
  font-family: var(--mr-font);
}

[data-testid="stSidebar"] {
  background: #ECEFF3;
  border-right: 1px solid var(--mr-border);
}

[data-testid="stSidebar"] h2 {
  font-size: 0.92rem !important;
  font-weight: 680 !important;
  letter-spacing: 0.04em !important;
  text-transform: uppercase;
}

[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
  color: var(--mr-secondary);
  font-size: 0.78rem;
  font-weight: 620;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

[data-testid="stSidebar"] input,
[data-testid="stSidebar"] [role="combobox"] {
  border-radius: 4px !important;
  border-color: var(--mr-border) !important;
  font-size: 0.86rem !important;
}

[data-testid="stHorizontalBlock"] {
  gap: 0.75rem;
}

[data-testid="stMetric"] {
  background: var(--mr-surface);
  border: 1px solid var(--mr-border);
  border-radius: 8px;
  box-shadow: 0 1px 2px rgba(25, 28, 31, 0.04);
  padding: 0.7rem 0.8rem;
}

[data-testid="stMetric"] label,
[data-testid="stMetricLabel"] {
  color: var(--mr-secondary) !important;
  font-size: 0.72rem !important;
  font-weight: 650 !important;
  letter-spacing: 0.04em !important;
  text-transform: uppercase;
}

[data-testid="stMetricValue"] {
  color: var(--mr-primary);
  font-family: var(--mr-font-data);
  font-size: 1.25rem !important;
  font-weight: 560 !important;
}

button[data-baseweb="tab"] {
  border-radius: 9999px !important;
  color: var(--mr-secondary) !important;
  font-size: 0.83rem !important;
  font-weight: 650 !important;
  padding: 0.2rem 0.7rem !important;
}

button[data-baseweb="tab"][aria-selected="true"] {
  background: var(--mr-primary) !important;
  border-bottom-color: transparent !important;
  color: white !important;
}

div[data-baseweb="tab-highlight"] {
  background: transparent !important;
  height: 0 !important;
}

div[data-baseweb="tab-border"] {
  background: var(--mr-border) !important;
}

[data-testid="stDataFrame"] {
  border: 1px solid var(--mr-border);
  border-radius: 8px;
  background: var(--mr-surface);
  overflow: hidden;
}

.mr-command-strip {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 0.55rem;
  margin: 0.35rem 0 1rem;
}

.mr-command-cell {
  background: var(--mr-surface);
  border: 1px solid var(--mr-border);
  border-radius: 8px;
  padding: 0.65rem 0.75rem;
}

.mr-command-label {
  color: var(--mr-secondary);
  display: block;
  font-size: 0.7rem;
  font-weight: 680;
  letter-spacing: 0.04em;
  line-height: 1.1;
  text-transform: uppercase;
}

.mr-command-value {
  color: var(--mr-primary);
  display: block;
  font-family: var(--mr-font-data);
  font-size: 1rem;
  font-weight: 560;
  line-height: 1.35;
  margin-top: 0.25rem;
  overflow-wrap: anywhere;
}

.mr-badge-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.45rem;
  margin: 0.35rem 0 0.75rem;
}

.mr-badge {
  align-items: center;
  background: var(--mr-surface-muted);
  border: 1px solid var(--mr-border);
  border-radius: 9999px;
  color: var(--mr-primary);
  display: inline-flex;
  font-size: 0.78rem;
  gap: 0.35rem;
  line-height: 1.15rem;
  padding: 0.24rem 0.55rem;
}

.mr-badge strong {
  color: rgba(25, 28, 31, 0.72);
  font-weight: 650;
}

.mr-badge-good {
  background: #EAF7F0;
  border-color: #A7D8BE;
  color: var(--mr-positive);
}

.mr-badge-warn {
  background: #FFF4D9;
  border-color: #E8C46C;
  color: var(--mr-warning);
}

.mr-badge-danger {
  background: #FCEBE8;
  border-color: #E9AAA2;
  color: var(--mr-danger);
}

@media (max-width: 900px) {
  .block-container {
    padding-left: 1rem;
    padding-right: 1rem;
  }

  .mr-command-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
"""


def dashboard_style() -> str:
    return DASHBOARD_STYLE


__all__ = ["DASHBOARD_STYLE", "dashboard_style"]

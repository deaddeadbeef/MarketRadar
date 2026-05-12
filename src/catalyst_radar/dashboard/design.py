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
  max-width: 1680px;
  margin-left: 0;
  margin-right: auto;
  padding-left: 1.35rem;
  padding-right: 1.35rem;
  padding-top: 0.8rem;
  padding-bottom: 2rem;
}

header[data-testid="stHeader"] {
  background: transparent;
  height: 0;
}

[data-testid="stToolbar"],
[data-testid="stDecoration"],
#MainMenu {
  display: none;
}

h1 {
  color: var(--mr-primary);
  font-size: 1.55rem !important;
  font-weight: 650 !important;
  line-height: 1.12 !important;
  letter-spacing: 0 !important;
  margin: 0 !important;
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
  gap: 0.65rem;
}

[data-testid="stMetric"] {
  background: var(--mr-surface);
  border: 1px solid var(--mr-border);
  border-radius: 8px;
  box-shadow: 0 1px 2px rgba(25, 28, 31, 0.04);
  padding: 0.62rem 0.72rem;
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

[data-testid="stTabs"] > div[role="tablist"] {
  align-items: center;
  background: var(--mr-surface);
  border: 1px solid var(--mr-border);
  border-radius: 8px;
  box-shadow: 0 1px 2px rgba(25, 28, 31, 0.035);
  display: flex;
  gap: 0.2rem;
  margin: 0.9rem 0 1rem;
  overflow-x: auto;
  padding: 0.24rem;
}

button[data-baseweb="tab"] {
  border-radius: 6px !important;
  color: var(--mr-secondary) !important;
  font-size: 0.83rem !important;
  font-weight: 650 !important;
  min-height: 2rem !important;
  padding: 0.28rem 0.72rem !important;
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
  background: transparent !important;
  height: 0 !important;
}

[data-testid="stDataFrame"] {
  border: 1px solid var(--mr-border);
  border-radius: 8px;
  background: var(--mr-surface);
  overflow: hidden;
}

[data-testid="stSelectbox"] label {
  color: var(--mr-secondary) !important;
  font-size: 0.78rem !important;
  font-weight: 660 !important;
  letter-spacing: 0.03em !important;
}

div[data-baseweb="select"] > div {
  background: var(--mr-surface) !important;
  border-color: var(--mr-border) !important;
  border-radius: 6px !important;
  color: var(--mr-primary) !important;
}

div[data-baseweb="select"] span,
div[data-baseweb="select"] svg {
  color: var(--mr-primary) !important;
}

.mr-table-wrap {
  background: var(--mr-surface);
  border: 1px solid var(--mr-border);
  border-radius: 8px;
  box-shadow: 0 1px 2px rgba(25, 28, 31, 0.035);
  margin: 0.25rem 0 1rem;
  overflow-x: auto;
}

.mr-table {
  border-collapse: collapse;
  color: var(--mr-primary);
  font-size: 0.86rem;
  min-width: 100%;
  width: max-content;
}

.mr-table th {
  background: #F1F4F7;
  border-bottom: 1px solid var(--mr-border);
  color: var(--mr-secondary);
  font-size: 0.72rem;
  font-weight: 680;
  letter-spacing: 0.03em;
  padding: 0.58rem 0.7rem;
  text-align: left;
  text-transform: uppercase;
  white-space: nowrap;
}

.mr-table td {
  border-bottom: 1px solid #E7EBF0;
  color: var(--mr-primary);
  line-height: 1.35;
  max-width: 26rem;
  padding: 0.58rem 0.7rem;
  vertical-align: top;
}

.mr-table tbody tr:last-child td {
  border-bottom: 0;
}

.mr-table-selected td {
  background: #F7FBF9;
  box-shadow: inset 3px 0 0 var(--mr-tertiary);
}

.mr-table-link {
  color: var(--mr-data-blue);
  font-weight: 620;
  text-decoration: none;
}

.mr-table-link:hover {
  text-decoration: underline;
}

.mr-chart-card {
  background: var(--mr-surface);
  border: 1px solid var(--mr-border);
  border-radius: 8px;
  box-shadow: 0 1px 2px rgba(25, 28, 31, 0.035);
  margin-top: 0.25rem;
  padding: 0.8rem;
}

.mr-chart-row + .mr-chart-row {
  border-top: 1px solid #E7EBF0;
  margin-top: 0.75rem;
  padding-top: 0.75rem;
}

.mr-chart-row-head {
  align-items: center;
  color: var(--mr-primary);
  display: flex;
  font-size: 0.86rem;
  justify-content: space-between;
  margin-bottom: 0.45rem;
}

.mr-chart-row-head strong {
  font-family: var(--mr-font-data);
  font-weight: 560;
}

.mr-chart-track {
  background: var(--mr-surface-muted);
  border-radius: 9999px;
  height: 0.65rem;
  overflow: hidden;
}

.mr-chart-bar {
  background: var(--mr-data-blue);
  border-radius: 9999px;
  display: block;
  height: 100%;
  min-width: 0.65rem;
}

.mr-chart-caption {
  color: var(--mr-secondary);
  display: block;
  font-size: 0.74rem;
  margin-top: 0.35rem;
}

.mr-inline-chip {
  background: var(--mr-surface-muted);
  border: 1px solid var(--mr-border);
  border-radius: 9999px;
  color: var(--mr-primary);
  display: inline-block;
  font-size: 0.75rem;
  font-weight: 560;
  line-height: 1.15;
  margin: 0.08rem 0.16rem 0.08rem 0;
  padding: 0.18rem 0.45rem;
  white-space: nowrap;
}

.mr-app-header {
  align-items: center;
  display: grid;
  gap: 1rem;
  grid-template-columns: minmax(16rem, 0.92fr) minmax(28rem, 1.7fr);
  margin: 0.15rem 0 0.3rem;
}

.mr-title-block {
  min-width: 0;
}

.mr-status-panel {
  align-items: center;
  background: var(--mr-surface);
  border: 1px solid var(--mr-border);
  border-radius: 8px;
  box-shadow: 0 1px 2px rgba(25, 28, 31, 0.035);
  display: grid;
  gap: 0.6rem;
  grid-template-columns: max-content 1fr;
  min-width: 0;
  padding: 0.42rem 0.5rem 0.42rem 0.65rem;
}

.mr-status-title {
  color: var(--mr-secondary);
  font-size: 0.68rem;
  font-weight: 720;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  white-space: nowrap;
}

.mr-command-strip {
  display: grid;
  gap: 0.28rem;
  grid-template-columns: repeat(5, minmax(5.6rem, 1fr));
  min-width: 0;
}

.mr-command-cell {
  background: #F8FAFC;
  border: 1px solid #E4E9EF;
  border-radius: 6px;
  min-width: 0;
  padding: 0.34rem 0.45rem;
}

.mr-command-cell-good {
  background: #F2FAF6;
  border-color: #CFECDD;
}

.mr-command-cell-warn {
  background: #FFF8E8;
  border-color: #F0D99A;
}

.mr-command-cell-danger {
  background: #FDF1EF;
  border-color: #F0C4BD;
}

.mr-command-label {
  color: var(--mr-secondary);
  display: block;
  font-size: 0.62rem;
  font-weight: 680;
  letter-spacing: 0.04em;
  line-height: 1.1;
  overflow: hidden;
  text-overflow: ellipsis;
  text-transform: uppercase;
  white-space: nowrap;
}

.mr-command-value {
  color: var(--mr-primary);
  display: block;
  font-family: var(--mr-font-data);
  font-size: 0.82rem;
  font-weight: 560;
  line-height: 1.2;
  margin-top: 0.1rem;
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

.mr-note-card {
  background: var(--mr-surface);
  border: 1px solid var(--mr-border);
  border-radius: 8px;
  box-shadow: 0 1px 2px rgba(25, 28, 31, 0.035);
  margin-top: 0.25rem;
  padding: 0.75rem 0.85rem;
}

.mr-note-row + .mr-note-row {
  border-top: 1px solid #E7EBF0;
  margin-top: 0.65rem;
  padding-top: 0.65rem;
}

.mr-note-label {
  color: var(--mr-secondary);
  display: block;
  font-size: 0.72rem;
  font-weight: 680;
  letter-spacing: 0.03em;
  margin-bottom: 0.25rem;
  text-transform: uppercase;
}

.mr-note-card p {
  color: var(--mr-primary);
  font-size: 0.94rem;
  line-height: 1.5;
  margin: 0 !important;
}

@media (max-width: 900px) {
  .block-container {
    padding-left: 1rem;
    padding-right: 1rem;
  }

  .mr-app-header {
    grid-template-columns: 1fr;
  }

  .mr-status-panel {
    align-items: stretch;
    grid-template-columns: 1fr;
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

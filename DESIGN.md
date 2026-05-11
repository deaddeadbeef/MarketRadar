---
version: alpha
name: Market Radar Command Center
description: Evidence-first investment decision dashboard for public-equity review.
colors:
  primary: "#191C1F"
  secondary: "#5C6670"
  tertiary: "#0B7A53"
  background: "#F7F8FA"
  surface: "#FFFFFF"
  surface-muted: "#EEF2F5"
  border: "#D9DEE5"
  data-blue: "#2563EB"
  positive: "#0B7A53"
  warning: "#A16207"
  danger: "#B42318"
  on-primary: "#FFFFFF"
  on-tertiary: "#FFFFFF"
  on-warning: "#4A3000"
  on-danger: "#FFFFFF"
typography:
  headline-lg:
    fontFamily: Inter, ui-sans-serif, system-ui, sans-serif
    fontSize: 32px
    fontWeight: 650
    lineHeight: 1.12
    letterSpacing: 0em
  headline-md:
    fontFamily: Inter, ui-sans-serif, system-ui, sans-serif
    fontSize: 21px
    fontWeight: 640
    lineHeight: 1.22
    letterSpacing: 0em
  body-md:
    fontFamily: Inter, ui-sans-serif, system-ui, sans-serif
    fontSize: 15px
    fontWeight: 400
    lineHeight: 1.55
    letterSpacing: 0em
  body-sm:
    fontFamily: Inter, ui-sans-serif, system-ui, sans-serif
    fontSize: 13px
    fontWeight: 400
    lineHeight: 1.45
    letterSpacing: 0em
  label-md:
    fontFamily: Inter, ui-sans-serif, system-ui, sans-serif
    fontSize: 12px
    fontWeight: 620
    lineHeight: 1.2
    letterSpacing: 0.04em
  data-md:
    fontFamily: IBM Plex Mono, ui-monospace, SFMono-Regular, Consolas, monospace
    fontSize: 14px
    fontWeight: 520
    lineHeight: 1.3
    letterSpacing: 0em
rounded:
  none: 0px
  xs: 2px
  sm: 4px
  md: 8px
  full: 9999px
spacing:
  xs: 4px
  sm: 8px
  md: 12px
  lg: 16px
  xl: 24px
  xxl: 32px
  content-max: 1440px
components:
  dashboard-shell:
    backgroundColor: "{colors.background}"
    textColor: "{colors.primary}"
    typography: "{typography.body-md}"
  surface-panel:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.primary}"
    rounded: "{rounded.md}"
    padding: "{spacing.lg}"
  metric-cell:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.primary}"
    typography: "{typography.data-md}"
    rounded: "{rounded.md}"
    padding: "{spacing.md}"
  badge-positive:
    backgroundColor: "#EAF7F0"
    textColor: "{colors.positive}"
    typography: "{typography.label-md}"
    rounded: "{rounded.full}"
    padding: "{spacing.sm}"
  badge-warning:
    backgroundColor: "#FFF4D9"
    textColor: "{colors.warning}"
    typography: "{typography.label-md}"
    rounded: "{rounded.full}"
    padding: "{spacing.sm}"
  badge-danger:
    backgroundColor: "#FCEBE8"
    textColor: "{colors.danger}"
    typography: "{typography.label-md}"
    rounded: "{rounded.full}"
    padding: "{spacing.sm}"
  input-field:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.primary}"
    typography: "{typography.body-sm}"
    rounded: "{rounded.sm}"
    padding: "{spacing.md}"
  tab-active:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.on-primary}"
    typography: "{typography.label-md}"
    rounded: "{rounded.full}"
    padding: "{spacing.sm}"
---

# Market Radar Command Center

## Overview

Market Radar should feel like a quiet institutional workstation for investment
review. It is dense enough for repeated use, but edited enough that risk,
evidence, and next actions can be scanned without visual fatigue.

The design voice is analytical, calm, and exact. It should not look like a
marketing site, a crypto terminal, or a decorative fintech mockup. The interface
exists to help an investor judge candidates, evidence quality, IPO/S-1 filings,
alerts, validation history, cost, and operational state.

## Colors

The palette uses graphite text, true white data surfaces, a cool neutral shell,
and distinct semantic colors.

- **Primary (`#191C1F`):** graphite for headings, core text, and active UI.
- **Secondary (`#5C6670`):** restrained gray for metadata and secondary labels.
- **Tertiary (`#0B7A53`):** sober green for useful, healthy, positive states.
- **Data Blue (`#2563EB`):** limited analytical accent for selected controls and
  links.
- **Warning (`#A16207`):** amber for planned, dry-run, stale, or watch states.
- **Danger (`#B42318`):** red for failure, critical, blocked, or degraded state.
- **Background (`#F7F8FA`):** cool neutral application shell.
- **Surface (`#FFFFFF`):** primary data surfaces and table containers.

## Typography

Use a system sans stack that approximates Inter. Headings are firm but not
oversized; controls and data labels are smaller and tighter. Use a monospace
stack only for prices, scores, timestamps, IDs, and other data-like values.

Headlines, table labels, tabs, sidebar labels, metric values, and badge text
must be explicitly styled. Do not rely on browser-default control typography.

## Layout

The app uses a command-center layout: a restrained sidebar for filters and a
wide main review surface capped at 1440px. Spacing follows an 8px-derived scale
with 12px and 24px as the main working rhythm.

Avoid nested cards and marketing-style panels. Use tables, metric strips,
compact status rows, and full-width review bands. The Overview tab should lead
with the candidate queue; detail tabs should lead with summary metrics before
supporting evidence.

## Elevation & Depth

Depth is mostly tonal. Use thin borders, subtle surface contrast, and restrained
shadows only for primary metric cells or focused table regions. Heavy shadows,
glows, and blurred decorative effects are not part of this product.

## Shapes

Rectangular UI uses a maximum 8px radius. Buttons, badges, and tabs may use a
full pill radius only when they represent a compact state or filter control.
Tables and panels should feel precise, not soft or bubbly.

## Components

- **Command Strip:** compact row under the title showing database health,
  candidate count, alert count, IPO/S-1 count, and degraded-mode state.
- **Metric Cells:** white, bordered, compact; values use data typography.
- **Status Badges:** semantic green, amber, red, or neutral; labels are strong
  and short.
- **Tables:** remain the primary visual surface for review queues and evidence.
  Do not convert queues into card grids.
- **Sidebar Controls:** quiet gray shell with clear labels and compact inputs.
- **Tabs:** active tab is high-contrast graphite; inactive tabs remain minimal.
- **Evidence Sections:** use tabular key/value rows before any raw payload.

## Do's and Don'ts

- Do keep the first screen focused on the actual review workflow.
- Do make ticker, state, score, alert status, and operational health immediately
  scannable.
- Do use semantic colors consistently across metrics, badges, alerts, and ops.
- Do preserve dense tables for investment review.
- Don't add hero sections, decorative orbs, bento grids, or marketing copy.
- Don't hide critical risk, degraded mode, failed jobs, or stale provider state.
- Don't use raw JSON as the first presentation of an artifact.
- Don't use more than one strong accent in a single local control group.

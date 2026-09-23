---
version: alpha
name: wx.jamestannahill.com
description: Live hyperlocal weather dashboard for a private station in Midtown Manhattan. Dark-only, instrument-panel presentation served as Astro SSR on Cloudflare Workers with a Preact uPlot chart island.
colors:
  bg: "#0a0a0a"
  surface: "#141414"
  border: "#222222"
  text: "#f0f0f0"
  muted: "#8e8e8e"
  faint: "#808080"
  accent: "#e8e0d0"
  anomaly: "#c8b97a"
  blue: "#5a9ad0"
typography:
  base:
    fontFamily: NHG Display
components:
  page:
    backgroundColor: "{colors.bg}"
    textColor: "{colors.text}"
    typography: "{typography.base}"
  card:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.text}"
  section-title:
    textColor: "{colors.muted}"
  source-tag:
    textColor: "{colors.faint}"
  control:
    textColor: "{colors.muted}"
  control-active:
    textColor: "{colors.accent}"
  focus-ring:
    textColor: "{colors.anomaly}"
---

## Overview

wx.jamestannahill.com publishes readings from one private weather station (callsign CATHAUS) in Midtown Manhattan, alongside in-house derived signals and a small set of labeled external sources. The interface reads like an instrument panel: a near-black ground, hairline borders, uppercase tracked labels, large tabular numerals, and a single warm gold reserved for what is unusual, live, or focused. The site is dark-only; there is no light theme and no `prefers-color-scheme` branch.

## Colors

- `bg` is the page ground on every route, and `theme-color` in the document head matches it. Canvas exports from the chart fill with the same value so shared images match the page.
- `surface` fills cards, chips, and rows that sit on `bg`. Sections are separated by `border` hairlines and 1px grid gaps, not by lighter panels.
- `text` is for primary readings. `muted` is the default for labels, sub-lines, and idle controls. `faint` is the floor for tertiary metadata (section meta, hints, source tags, legend captions). Nothing on the page is set fainter than `faint`; both muted tiers are chosen to clear 4.5:1 on `bg`.
- `accent` is the interaction color: hover and active states on controls, selected chart metric and range, jump-row hover.
- `anomaly` is the signal color: departures from normal, the live dot, the TODAY summary rule, the chart's observed series and now-line, the chart tooltip timestamp, clickable-card hover rule, copy/share confirmation flash, and the keyboard focus outline. Do not spend it on decoration.
- `blue` is the precipitation and "cool/forward" color: rain amounts in the hourly strip, nearby chips, and rain events; the chart rain series; the TOMORROW summary rule and date; and the Celsius state of the unit toggle. All rain readouts use `blue`; there is no separate green rain color.
- Warning states sit outside the token set: the stale-data banner uses a translucent amber and NWS alerts use a darker amber panel. Network-spread banding uses a three-step ramp (muted, gold, orange) shared by the variance readout and nearby chips.
- The comfort calendar colors each day on an HSL hue ramp from red (least comfortable) through yellow to green (most comfortable) at fixed saturation and lightness; the legend bar uses the same ramp. Day labels on the cells are white.

## Typography

- One family, NHG Display (Neue Haas Grotesk Display Pro), loaded from the owner's font host, with the system sans as fallback. The chart island and the OG image renderer name the same family so canvas text matches DOM text.
- Section titles, card labels, station name, and source tags are small uppercase text with wide letter-spacing (roughly 0.08em to 0.12em) at weight 500. Write these labels in capitals in markup rather than relying on `text-transform`, except where the class already applies it (jump row, source tags, climate metric labels).
- Readings use large sizes at weight 400 with slight negative tracking. Every number that updates in place (hero temperature, card values, forecast, records, nearby temperatures, timestamps, rain amounts) sets `font-variant-numeric: tabular-nums` so values do not shift width on refresh.
- The hero temperature scales fluidly with `clamp()` and has its own smaller clamp below 600px.
- Italic is reserved for the anomaly headline and the network verdict line; both are interpretive statements rather than readings.
- API strings that join clauses with a middle dot or dash are rewritten to comma-separated sentences before display.

## Layout

- Single centered column capped at 900px, with horizontal padding that respects safe-area insets. Sections stack with a consistent large bottom margin.
- Metric groups (conditions, analog forecast, records) are 3-column grids with a 1px gap inside a 1px border, which produces hairline dividers between `surface` cells. At 600px and below they collapse: conditions and records to 2 columns (an odd last card spans both), forecast to a single-row-per-offset layout.
- Every section header is a flex row with the uppercase title on the left and meta text plus a source tag on the right.
- Horizontal overflow rows (jump nav, hourly strip) scroll without a visible scrollbar; the jump nav adds a right-edge mask fade with matching right padding so the last item can scroll clear of it.
- Anchor targets carry a scroll margin so jump-nav links do not land flush against the viewport edge.
- Lazily filled sections render server-side skeletons sized like the real content, and the chart frame reserves its loaded height up front, so late data does not shift layout.

## Elevation & Depth

- The page is flat. Depth comes from the `bg` / `surface` step and `border` hairlines, not shadows.
- Only floating overlays cast a shadow: the viewport-aware page tooltip and the chart cursor tooltip. Both use a dark surface, a slightly lighter border, and a soft black shadow.

## Shapes

- Corners are square by default: cards, buttons, chips, source tags, the chart frame, and banners have no radius. Small radii (1px to 3px) appear only on tiny marks such as comfort cells, legend swatches, bar tracks, the alerts banner, and the WeatherKit logo. The live dot and deviation-bar markers are circles.

## Components

- Buttons are outline-only: transparent fill, `border` hairline, `muted` text; hover (fine pointers only) and active states move both border and text to `accent`. Selected chart metric and range buttons hold the `accent` state and expose `aria-pressed`.
- Every pressable element answers a press with a quick scale-down on the shared `--ease-out` curve (0.97 for buttons, 0.99 for clickable cards). Under reduced motion the scale is removed and only color transitions remain.
- Hover styles are wrapped in `(hover: hover) and (pointer: fine)` so touch devices never show sticky hover.
- Tap targets are 44px on touch: header share and refresh buttons, chart metric and range buttons, and chart action buttons under `pointer: coarse`. The header compacts its buttons below 600px.
- Clickable condition cards are real `<button>` elements with the button chrome reset; hovering or pressing them raises an `anomaly` top border and selects that metric in the history chart. Cards without a chart series stay plain `div`s.
- Source tags label provenance on every data section. In-house readings are tagged CATHAUS, in-house derivations CATHAUS, COMPUTED, and both link to the docs anchor. External sources (WeatherKit, NOAA, Weather Underground) use the `source-external` variant, a cool blue-grey text and border, and WeatherKit carries its logo.
- Tooltips are triggered by `.has-tooltip` with `data-tooltip` text, positioned by script to stay inside the viewport; non-focusable tooltip hosts are given a tab stop so keyboard users can reach them. Tooltip hosts use `cursor: help`.
- The history chart is a uPlot canvas on a slightly lifted dark frame. The observed series is a 2px `anomaly` line with a faint fill; the historical baseline is a dashed white line with a translucent blue-grey plus or minus 1 sigma band; rain is a translucent blue area on its own hidden scale; a dashed `anomaly` vertical marks now. The legend below mirrors these marks and only lists baseline and rain when present. The canvas carries `role="img"` with a text summary of low, high, and latest values, and a load failure shows a plain-language message with a retry button.
- Icon glyphs are single-stroke SVGs at 1.8 stroke width in `currentColor`. Hourly conditions use a restrained text-presentation glyph set (quarter-filled circles for cloud cover, forced text style for rain and snow) instead of emoji.
- `.sr-only` supplies screen-reader text where the visual carries meaning only by color or position, such as comfort cell details and the unit toggle's purpose.

## Motion

- One easing curve, `--ease-out` (a strong ease-out), governs presses, banners, tooltips, and scroll reveal. Durations are short: roughly 120ms to 300ms.
- Entrances use `@starting-style` (stale banner, tooltip) and a small upward reveal for sections that fade in on scroll. Cross-page navigation uses a brief root view-transition fade.
- Continuous animation is limited to the live-dot pulse, skeleton shimmer, and the refresh icon spin (the icon rotates, not its bordered box).
- Every animation has a `prefers-reduced-motion: reduce` fallback that removes movement: no press scale, no pulse, no shimmer, no reveal offset, no view-transition fade, and no smooth scroll.

## Do's and Don'ts

- Do keep text at or above the `faint` tier; do not introduce a dimmer grey for body or metadata text.
- Do show rain in `blue` everywhere it appears.
- Do give every data section a source tag that states whether the value is measured, computed in-house, or external.
- Don't show hover styling on touch-only devices.
- Don't render condition glyphs as color emoji.

## Open Questions

- Dew point in the climate panel uses the same blue as the low-temperature row, so "cool" and "moisture" read as one series. Decide whether dew point needs its own hue or should stay muted as it already is in daily mode.
- The CATHAUS source tag repeats on nearly every section (today, yesterday, conditions, calendar, analog forecast, history, records). Decide whether provenance should be stated once per page with only exceptions tagged, or kept per section as it is now.
- The comfort calendar uses a red to green hue ramp, which is hard to separate for red-green color-vision deficiency even with white day labels and tooltips. Decide whether to move to a single-hue or blue to gold ramp.
- The climate panel's metric colors (a brighter yellow for temperature, a cyan for low temp and dew point) and the embed page's link and body colors are page-local literals outside the token set. Decide whether they should map onto `anomaly`, `blue`, and `muted`, or become named tokens.
- Docs, 404, and embed pages each restate base styles locally; 404 does not load the shared stylesheet. Decide whether they should consume the shared tokens.

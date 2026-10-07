---
version: alpha
name: wx.jamestannahill.com
description: Live hyperlocal weather dashboard for a private station in Midtown Manhattan. Dark-only, instrument-panel presentation served as Astro SSR on Cloudflare Workers with a Preact uPlot chart island.
colors:
  bg: "#0a0a0a"
  surface: "#141414"
  border: "#222222"
  text: "#f0f0f0"
  text-2: "#b4b4b4"
  surface-hover: "#161616"
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
- `text-2` is a softer reading tier for running prose (summary paragraphs) and tooltip bodies; `surface-hover` is the hover fill on clickable cards.
- `text` is for primary readings. `muted` is the default for labels, sub-lines, and idle controls. `faint` is the floor for tertiary metadata (section meta, hints, source tags, legend captions). Nothing on the page is set fainter than `faint`; both muted tiers are chosen to clear 4.5:1 on `bg`.
- `accent` is the interaction color: hover and active states on controls, selected chart metric and range, jump-row hover. On the docs page it is also the text color of body links (paragraphs, list items, table cells), which keep an underline in a dim grey that turns `accent` on hover.
- `anomaly` is the signal color: departures from normal, the live dot, the TODAY summary rule, the chart's observed series and now-line, the chart tooltip timestamp, clickable-card hover rule, copy/share confirmation flash, and the keyboard focus outline. Do not spend it on decoration.
- `blue` is the precipitation and "cool/forward" color: rain amounts in the hourly strip, nearby chips, and rain events; the chart rain series; the TOMORROW summary rule and date; and the Celsius state of the unit toggle. All rain readouts use `blue`; there is no separate green rain color.
- Warning states sit outside the token set and share one amber (#d1974a): the stale-data banner on a faint amber tint, NWS alerts on a slightly stronger one. Network-spread banding uses a three-step ramp (muted, gold, orange) shared by the variance readout and nearby chips.
- The comfort calendar colors each day on an HSL hue ramp from red (least comfortable) through yellow to green (most comfortable) at fixed saturation and lightness; the legend bar uses the same ramp. Day labels on the cells are white.

## Typography

- One family, NHG Display (Neue Haas Grotesk Display Pro), loaded from the owner's font host on every route, including the standalone embed and 404 pages, with the system sans as fallback. The chart island and the OG image renderer name the same family so canvas text matches DOM text.
- Section titles, card labels, station name, and source tags are small uppercase text with wide letter-spacing (roughly 0.08em to 0.12em) at weight 500. Write these labels in capitals in markup rather than relying on `text-transform`, except where the class already applies it (jump row, source tags, climate metric labels).
- Readings use large sizes at weight 400 with slight negative tracking. Every number that updates in place (hero temperature, card values, forecast, records, nearby temperatures, timestamps, rain amounts) sets `font-variant-numeric: tabular-nums` so values do not shift width on refresh.
- The hero temperature scales fluidly with `clamp()` and has its own smaller clamp below 600px.
- Italic is reserved for the anomaly headline and the network verdict line; both are interpretive statements rather than readings.
- The docs page adds an h3 step between h2 and body text at weight 500 in `text`. Headings there balance their wrapping and paragraphs use pretty wrapping.
- Type sizes come from a short scale: 11px labels and metadata (the floor for any functional text, source tags included), 12 to 13px secondary prose, 15px small readings, 22px secondary readings (forecast, records, nearby), 30px condition readings (22px on phones), and the fluid hero. Uppercase labels are weight 500 with 0.1em tracking; mixed-case text is never letter-spaced.
- Prose blocks (summaries, footer) are capped at about 70 characters per line.
- The hero unit is sized in em against the numeral (0.3em), so it scales with the clamp on `.temp-block`.
- The docs page sets code in the system monospace stack (SF Mono, Fira Code, monospace); it is the only second family and appears only in code.
- Every time and date reads in station time (America/New_York): clock times as "10:35 AM", axis hours as "10 AM", chart dates as "10/6", record dates as "Jul 2, 2026".
- API strings that join clauses with a middle dot or dash are rewritten to comma-separated sentences before display.

## Layout

- Page order: header, jump nav, hero, then the Conditions grid, so live readings sit above the fold; the climate panel and daily summaries follow.
- Single centered column capped at 900px, with horizontal padding that respects safe-area insets. Sections stack with a consistent large bottom margin.
- Metric groups (conditions, analog forecast, records) are 3-column grids with a 1px gap; each cell draws a 1px `--hairline` box-shadow ring, so neighbouring rings overlap in the gap and every divider and outer edge is a single pixel (rain rows use the same rule). At 600px and below they collapse: conditions and records to 2 columns (an odd last card spans both), forecast to a single-row-per-offset layout.
- Every section header is a flex row with the uppercase title on the left and meta text plus a source tag on the right, separated from its content by one `--head-gap` (12px).
- Horizontal overflow rows (jump nav, hourly strip) scroll without a visible scrollbar; the jump nav adds a right-edge mask fade with matching right padding so the last item can scroll clear of it.
- Anchor targets carry a scroll margin so jump-nav links do not land flush against the viewport edge.
- On the docs page, nothing may widen the page at 320px: paragraphs and table cells break long strings, tables scroll inside their own block, and code blocks scroll horizontally and are focusable so keyboard users can scroll them.
- Lazily filled sections render server-side skeletons sized like the real content, and the chart frame reserves its loaded height up front, so late data does not shift layout.

## Elevation & Depth

- The page is flat. Depth comes from the `bg` / `surface` step and `border` hairlines, not shadows.
- Only floating overlays cast a shadow: the viewport-aware page tooltip and the chart cursor tooltip. Both use a dark surface, a slightly lighter border, and a soft black shadow.

## Shapes

- Corners are square by default: cards, buttons, chips, source tags, the chart frame, and banners have no radius. Small radii (1px to 3px) appear only on tiny marks such as comfort cells, legend swatches, bar tracks, and the WeatherKit logo. The live dot and deviation-bar markers are circles.

## Components

- Buttons are outline-only: transparent fill, `border` hairline, `muted` text; hover (fine pointers only) and active states move both border and text to `accent`. Selected chart metric and range buttons are filled: `surface` background, `accent` border, `text` label, so a hovered neighbour never reads as selected; they expose `aria-pressed`.
- Every pressable element answers a press with a quick scale-down on the shared `--ease-out` curve (0.97 for buttons, 0.99 for clickable cards). Under reduced motion the scale is removed and only color transitions remain.
- Hover styles are wrapped in `(hover: hover) and (pointer: fine)` so touch devices never show sticky hover. This applies to the docs and 404 pages as well as the dashboard.
- Tap targets are 44px on touch: header share and refresh buttons, chart metric and range buttons, and chart action buttons under `pointer: coarse`. The header compacts its buttons below 600px.
- Clickable condition cards are real `<button>` elements with the button chrome reset; hovering or pressing them raises an inset `anomaly` top rule and selects that metric in the history chart. Cards without a chart series stay plain `div`s.
- Source tags label provenance on every data section. In-house readings are tagged CATHAUS, in-house derivations CATHAUS, COMPUTED, and both link to the docs anchor. External sources (WeatherKit, NOAA, Weather Underground) use the `source-external` variant, a cool blue-grey text and border, and WeatherKit carries its logo.
- Tooltips are triggered by `.has-tooltip` with `data-tooltip` text, positioned by script to stay inside the viewport; non-focusable tooltip hosts are given a tab stop so keyboard users can reach them. Tooltip hosts use `cursor: help`. On hover the first tip waits 300ms; while one is open the next shows instantly. Native `title` tooltips are not used.
- The history chart is a uPlot canvas on a slightly lifted dark frame. The observed series is a 2px `anomaly` line with a faint fill; the historical baseline is a dashed white line with a translucent blue-grey plus or minus 1 sigma band; rain is a translucent blue area on its own hidden scale; a dashed `anomaly` vertical marks now. The legend below mirrors these marks and only lists baseline and rain when present. The canvas carries `role="img"` with a text summary of low, high, and latest values, and a load failure shows a plain-language message with a retry button. A plain wheel scrolls the page; the wheel zooms only with Ctrl held (which also covers trackpad pinch) or once the chart is already zoomed, and the on-chart hint says so.
- Icon glyphs are single-stroke SVGs at 1.8 stroke width in `currentColor`. Hourly conditions use a restrained text-presentation glyph set (quarter-filled circles for cloud cover, forced text style for rain and snow) instead of emoji.
- `.sr-only` supplies screen-reader text where the visual carries meaning only by color or position, such as comfort cell details, hourly cell details, and the unit toggle's purpose. Its absolutely positioned text must sit inside a positioned ancestor within any scroller (hourly cells and the hourly strip are positioned for this), or it escapes the scroller and widens the page.
- The embed widget (`widget.js`) inherits the host page's font and color, sets tabular numerals, and lays its readings out as a wrapping inline row where each item stays unbroken, so it wraps cleanly on narrow hosts. A missing temperature or failed request shows the text "Weather unavailable" rather than an empty badge or a placeholder zero.

## Motion

- One easing curve, `--ease-out` (a strong ease-out), governs presses, banners, and tooltips. Durations are short: roughly 120ms to 300ms.
- Entrances use `@starting-style` (stale banner, tooltip); there is no scroll reveal. Cross-page navigation uses a brief root view-transition fade.
- Continuous animation is limited to the live-dot pulse, skeleton shimmer, and the refresh icon spin (the icon rotates, not its bordered box).
- Every animation has a `prefers-reduced-motion: reduce` fallback that removes movement: no press scale, no pulse, no shimmer, no view-transition fade, no smooth scroll, and no refresh spin (the icon holds still at reduced opacity while refreshing).

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
- The climate panel's temperature color (a brighter yellow; low temp and dew point now use `blue`) and the embed page's link and body colors are page-local literals outside the token set. Decide whether they should map onto `anomaly`, `blue`, and `muted`, or become named tokens.
- Docs, 404, and embed pages each restate base styles locally, and 404 still does not load the shared stylesheet (it loads only the font). Decide whether they should consume the shared tokens.

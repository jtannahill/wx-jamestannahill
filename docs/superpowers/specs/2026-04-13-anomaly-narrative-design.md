# Anomaly Narrative — Design Spec
**Date:** 2026-04-13  
**Project:** wx.jamestannahill.com  
**Status:** Approved

---

## Overview

Add historical climate context to the wx dashboard: a punchy anomaly headline and a "Today in History" percentile panel, both powered by 85–156 years of real data. The panel operates in two modes — live (current reading vs. historical hourly distribution) and daily verdict (today's confirmed high/low vs. the longest record available).

---

## Data Sources

| Source | Depth | Granularity | Fields | Powers |
|---|---|---|---|---|
| NOAA GHCN-Daily, Central Park (USC00305801) | 1869–present (156 yrs) | Daily | TMAX, TMIN, AWND | "Warmest since YEAR" headline, daily verdict percentiles for temp + wind |
| ERA5 via Open-Meteo Archive | 1940–present (85 yrs) | Hourly | temp, dewpoint, wind | Live percentile panel, dew point history (all modes) |

**Note on dew point:** NOAA GHCN-Daily does not include dew point. Dew point percentiles use ERA5 (85 years) in both live and daily modes. The daily verdict does not produce a "wettest since YEAR" claim for dew point.

---

## New DynamoDB Tables

### `wx-climate-doy`
- **Key:** `doy` (string, e.g. `"0413"`) — 366 rows
- **Source:** NOAA GHCN-Daily
- **Fields per row:**
  - `annual_highs`: list of `{year, tmax, tmin, awnd}` for all years 1869–present, sorted by year descending. Used to find `last_exceeded_year`: scan until a year whose tmax > today's high is found.
  - `p5_tmax, p25_tmax, p50_tmax, p75_tmax, p95_tmax`
  - `p5_tmin, p25_tmin, p50_tmin, p75_tmin, p95_tmin`
  - `p5_awnd, p25_awnd, p50_awnd, p75_awnd, p95_awnd`
  - `record_high_temp, record_high_year`
  - `record_low_temp, record_low_year`
  - `record_high_wind, record_high_wind_year`
  - `sample_count` (years with valid data for this date)

### `wx-climate-hourly`
- **Key:** `doy_hour` (string, e.g. `"0413-14"`) — 8,784 rows (366 × 24). Hour is 0–23 in America/New_York local time, matching the existing `month_hour` convention.
- **Source:** ERA5 via Open-Meteo Archive
- **Fields per row:**
  - `p25_tempf, p50_tempf, p75_tempf, mean_tempf, std_tempf`
  - `p25_dewptf, p50_dewptf, p75_dewptf, mean_dewptf, std_dewptf`
  - `p25_windmph, p50_windmph, p75_windmph, mean_windmph, std_windmph`
  - `sample_count` (~85)

---

## New Lambdas

### `wx_climate_bootstrap`
- **Trigger:** Manual (one-time, re-runnable — idempotent writes)
- **Config:** 1024MB, 900s timeout, arm64
- **Step 1 — NOAA:** Download Central Park CSV from NOAA CDN. Parse all rows. Group by MM-DD. Compute percentile buckets + sorted annual lists. Write 366 rows to `wx-climate-doy`.
- **Step 2 — ERA5:** Loop over 12 months. For each, fetch full hourly range 1940–2024 from Open-Meteo Archive. Group by MM-DD-HH. Compute p25/p50/p75/mean/std for temp, dewpoint, wind. Write ~8,784 rows to `wx-climate-hourly`.
- **Duration:** ~10 min total.

### `wx_climate_updater`
- **Trigger:** EventBridge nightly at 06:00 UTC (after `wx_summarizer`)
- **Config:** 512MB, 120s timeout, arm64
- **Daily:** Fetch ERA5 for yesterday's MM-DD. Recompute hourly stats for that DOY slot and update `wx-climate-hourly`.
- **Monthly (1st of month):** Re-download NOAA CSV to pick up the trailing ~2-week data lag. Recompute and update `wx-climate-doy` for all 366 slots.

---

## API Changes

### `GET /current` — new `climate_context` field

```json
{
  "climate_context": {
    "mode": "live",
    "headline": "Currently 97th percentile for 3pm on April 13th in 85 years of records",
    "metrics": {
      "temp": {
        "value": 72.1,
        "percentile": 97,
        "label": "97th percentile for this hour",
        "p25": 58.2, "p50": 62.8, "p75": 68.1,
        "years_of_data": 85
      },
      "dewpoint": { },
      "wind": { }
    },
    "verdict": null
  }
}
```

When `mode = "daily"` (today's summary exists in `wx-daily-summaries`):

```json
{
  "climate_context": {
    "mode": "daily",
    "headline": "Warmest April 13th since 1923 · 98th percentile",
    "metrics": { },
    "verdict": {
      "temp_high": {
        "value": 72.1,
        "percentile": 98,
        "last_exceeded_year": 1923,
        "label": "Warmest April 13th since 1923",
        "years_of_data": 156
      },
      "temp_low": { },
      "wind_high": { },
      "dewpoint_high": {
        "years_of_data": 85
      }
    }
  }
}
```

**Mode switching:** `verdict` is null until today's entry exists in `wx-daily-summaries` (written by `wx_summarizer` at 05:00 UTC). `metrics` (live percentiles) is always populated regardless of mode — it reflects the current reading. When `verdict` is available, the dashboard displays it prominently and renders `metrics` in a secondary role. The `mode` field signals which to emphasize.

**Performance:** Two DynamoDB `get_item` point reads added to `/current`. No ERA5 calls at runtime. <5ms added latency.

### New module: `wx_api/climate_context.py`

Three functions:

- `live_context(reading, doy_hour_stats)` — computes percentile rank for temp/dewpoint/wind vs. ERA5 distribution. Returns label: "97th percentile for 3pm on April 13th in 85 years of records".
- `daily_verdict(today_high, today_low, doy_stats)` — ranks today's confirmed high/low against NOAA 156-year distribution. Scans `annual_highs` list to find `last_exceeded_year`.
- `anomaly_headline(live, verdict)` — selects the more prominent claim and formats the punchy headline string. Prefers `verdict` when available.

---

## Dashboard Changes

### Updated anomaly headline
The existing `#anomaly-headline` element gains a subtitle line (smaller text, dimmer color) showing the historical context:
- Live: `"7.3°F above average for 3pm in April"` + subtitle `"Currently 97th percentile for this hour · 85 yrs"`
- Daily: `"Warmest April 13th since 1923"` + subtitle `"98th percentile · Central Park records since 1869"`

### New "Today in History" panel
Placed immediately below the anomaly headline section, above the comfort calendar.

**Live mode** (ERA5, during the day):
- 3 metric rows: Temperature, Dew Point, Wind Speed
- Each row: value + percentile label + gradient progress bar + p25/p50/p75 tick marks
- Footer: `ERA5 1940–2024 · 85 yrs`

**Daily verdict mode** (after midnight, NOAA + ERA5):
- 4 metric rows: High Temp, Low Temp, Avg Wind (all NOAA AWND = daily average wind speed, 156 yrs), Dew Point peak (ERA5, 85 yrs, slightly muted). Note: NOAA AWND is daily average wind, not peak gust — labeled "Avg Wind" to avoid confusion with the station's max gust records.
- High Temp row shows `last_exceeded_year` inline: `"98th pct · since 1923"`
- Dew Point row has subtle visual treatment + "85 yrs (ERA5)" note
- Footer: `NOAA Central Park 1869–2024 · 156 yrs`

Both modes use the existing dashboard color palette:
- Temperature: `#e8c84a` (amber)
- Dew point: `#4ab8e8` (blue)
- Wind: `#888` (neutral)

### New `app.js` function: `renderClimatePanel(data)`
Reads `data.climate_context`. Renders live or daily layout depending on `mode`. Attaches to the existing 5-min polling loop — panel updates every refresh.

---

## Placement in existing pipeline

```
wx_poller (5-min)         — unchanged
wx_summarizer (05:00 UTC) — unchanged; wx_climate_updater runs after it
wx_climate_updater (06:00 UTC) — NEW: updates yesterday's DOY slot
wx_api /current           — +2 DynamoDB reads, +climate_context in response
dashboard app.js          — +renderClimatePanel(), updated anomaly headline
```

---

## Out of Scope
- Rain / precipitation history (ERA5 precipitation is modeled, not station-measured; excluded)
- UV / solar radiation history (not available in ERA5 at this depth)
- Chart overlay of historical distribution (possible future enhancement)
- "Warmest since YEAR" for dew point (no NOAA source; ERA5 only gives percentile)

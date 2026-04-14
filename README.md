# wx.jamestannahill.com

Live hyperlocal weather dashboard for Midtown Manhattan, New York. Data from a private **Ambient Weather WS-2902** station, updated every 5 minutes.

**Live:** [wx.jamestannahill.com](https://wx.jamestannahill.com) · **API:** [api.wx.jamestannahill.com/current](https://api.wx.jamestannahill.com/current)

---

## Features

### Dashboard
- **Live conditions** — temperature, feels-like, humidity, dew point, wind, gusts, pressure, UV index, solar radiation, rainfall
- **Anomaly headline** — how today compares to the station's own rolling baseline for this month and hour
- **Percentile rank** — where current temperature falls in the station's historical distribution
- **Yesterday's summary** — prose recap of the prior day (high/low, rain events, notable winds), auto-generated nightly
- **Today in History** — daily verdict panel backed by 156 years of NOAA Central Park records and 85 years of ERA5 reanalysis. Shows whether today's high and low are hotter/cooler/normal vs. the historical average, with deviation bars spanning p5→p95 in actual °F and a tick at the long-run mean
- **WeatherKit forecast** — Apple WeatherKit 7-day outlook with daily high/low, condition, precipitation probability, and UV index
- **NWS alerts** — active National Weather Service warnings and advisories for the station area
- **Nearby stations** — personal weather stations in the area with current conditions for local comparison
- **Comfort calendar** — 30-day heatmap grid, color-coded by daily comfort score (0–100)
- **Station records** — current-month extremes: temp high/low, max gust, peak rain rate, pressure range, each with the date it was set
- **Analog forecast** — +1h/+2h/+3h predictions via nearest-neighbor pattern matching on 90 days of station data, with running MAE accuracy
- **Urban Heat Island delta** — this station vs. the average of JFK, LGA, and EWR METAR readings, updated every 5 minutes
- **Rain probability** — logistic regression model fitted weekly on labeled station history, with persistence boost if currently raining

### Chart
- Multi-field selector: Temperature, Humidity, Wind, Pressure, Urban Heat
- Time ranges: 24h / 7d / 30d with automatic downsampling
- Historical baseline overlay (dotted line)
- **±1σ anomaly bands** — shaded region between ±1 standard deviation of the baseline, derived from Welford's online variance computed across 90 days of readings
- Rendered with **uPlot** for high-performance canvas drawing

### API
Public, read-only, no authentication required.

| Endpoint | Description |
|---|---|
| `GET /current` | Latest reading with all ML signals and metadata |
| `GET /history?hours=N` | Last N hours of readings (max 720), downsampled for longer ranges, with per-slot baselines and std dev |
| `GET /rain-events?days=N` | Parsed rain events from the last N days (default 30) |
| `GET /daily-summaries?days=N` | Pre-computed daily summaries from the last N days (default 30) |

---

## Architecture

```
Ambient Weather WS-2902
        │  (Wi-Fi, every 5 min)
        ▼
Ambient Weather Cloud API
        │
        ▼
wx-poller (Lambda, EventBridge 5-min)
   • Fetches reading
   • Range + stuck-sensor validation
   • NOAA METAR fetch → UHI delta
   • Welford rolling stats (mean + variance per MM-HH slot)
   • Writes to wx-readings (90-day TTL)
        │
        ├──► wx-forecaster (Lambda, 30-min)
        │      • Nearest-neighbor analog forecast on 90-day hourly data
        │      • Evaluates previous forecast accuracy → MAE stored
        │
        ├──► wx-ml-fitter (Lambda, weekly Sun 3AM UTC)
        │      • Scans 90-day readings, labels rain events
        │      • Fits logistic regression w/ class weighting
        │      • Stores coefficients to wx-ml-models if F1 ≥ 0.05
        │
        ├──► wx-summarizer (Lambda, daily 5AM UTC)
        │      • Processes last 30 days (idempotent)
        │      • Computes daily stats + prose summary
        │      • Writes to wx-daily-summaries
        │
        ├──► wx-records-tracker (Lambda, weekly Sun 2AM UTC)
        │      • Scans 90-day readings
        │      • Computes per-month station records
        │      • Writes to wx-station-records
        │
        ├──► wx-alerter (Lambda, 15-min)
        │      • Evaluates anomaly thresholds
        │      • Sends SES email on trigger (2h cooldown)
        │
        ├──► wx-climate-bootstrap (Lambda, one-time)
        │      • Backfills 156yr NOAA Central Park daily records (noaa-ghcn-pds S3)
        │      • Backfills 85yr ERA5 reanalysis via Open-Meteo
        │      • Writes to wx-climate-normals
        │
        └──► wx-climate-updater (Lambda, daily 6AM UTC)
               • Appends yesterday's NOAA + ERA5 observations
               • Keeps climate baseline current

wx-api (Lambda, API Gateway HTTP API → CloudFront)
   • /current   — live reading + all ML signals + climate context + daily summary + records
   • /history   — downsampled readings with baselines + std dev
   • /rain-events — parsed rain events
   • /daily-summaries — pre-computed day summaries

S3 + CloudFront → wx.jamestannahill.com  (dashboard)
API Gateway + CloudFront → api.wx.jamestannahill.com
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Station | Ambient Weather WS-2902, Wi-Fi gateway |
| Compute | AWS Lambda (Python 3.12, arm64) |
| Scheduling | AWS EventBridge |
| Storage | AWS DynamoDB (on-demand, 10 tables) |
| API | AWS API Gateway HTTP API |
| CDN | AWS CloudFront (API + dashboard) |
| Dashboard | Vanilla JS, uPlot, NHG Display font |
| IaC | AWS CDK (Python) |
| Email | AWS SES |
| Secrets | AWS Secrets Manager |
| Climate data | 156yr NOAA GHCN-Daily (Central Park) + 85yr ERA5 reanalysis |
| Forecast | Apple WeatherKit API |
| Active alerts | NOAA National Weather Service API |
| Airport METAR | NOAA aviationweather.gov |

---

## DynamoDB Tables

| Table | Key | Purpose |
|---|---|---|
| `wx-readings` | station_id + timestamp | Raw 5-min readings, 90-day TTL |
| `wx-daily-stats` | station_id + month_hour | Rolling mean + variance per MM-HH baseline slot |
| `wx-alerts` | alert_type | Alert debounce state |
| `wx-forecasts` | station_id | Latest pre-computed analog forecast |
| `wx-forecast-accuracy` | station_id + evaluated_at | Per-evaluation MAE + running mean |
| `wx-uhi-seasonal` | station_id + month | Monthly rolling UHI delta averages |
| `wx-ml-models` | model_id | Fitted logistic regression weights + metrics |
| `wx-daily-summaries` | station_id + date | Daily stats + prose summary |
| `wx-station-records` | station_id + month | Per-month extreme records with dates |
| `wx-climate-normals` | station_id + date | 156yr NOAA + 85yr ERA5 historical daily normals |

---

## ML Details

### Rain Probability
Logistic regression: `p = σ(w·x + b)`. Features: humidity (normalized), 1-hour pressure trend, dew-point depression, sin/cos of hour-of-day. Weekly refit via gradient descent (300 epochs, L2 regularization, class weighting `n_neg/n_pos`). Current model: F1=0.396, recall=82%, n=24,348.

### Analog Forecast
1-hour buckets of the last 90 days → normalize to [0,1] → 6-hour fingerprint → nearest 5 Euclidean neighbors → average their next 3-hour trajectories. Confidence = inverse of mean neighbor distance, normalized to 0–100%.

### Baseline Variance (±1σ bands)
Welford's online algorithm accumulates per-slot variance alongside the rolling mean. Transition to EMA variance at 8,640 samples (30 days). Standard deviation is stored as `std_{field}` per MM-HH slot and served via `/history`.

### Today in History (Climate Context)
On each `/current` call, `climate_context.py` loads the NOAA + ERA5 normals for today's calendar date and computes a `daily_verdict` for both `temp_high` and `temp_low`. Each verdict includes: the observed value, the long-run p5/p50/p95 percentiles in °F, a z-score, and a label (much hotter / warmer / normal / cooler / much colder). The dashboard renders deviation bars that start at the historical average and extend to today's value, with the full p5→max(p95, value) range as the track.

---

## Repo Structure

```
lambdas/
  shared/           # DynamoDB client, Secrets Manager, UHI fetch
  wx_poller/        # 5-min data collection + OG image
  wx_api/           # REST API + all signal computation
  wx_alerter/       # Anomaly alert emails
  wx_bootstrap/     # One-time ERA5 + WU backfill (readings)
  wx_climate_bootstrap/ # One-time NOAA GHCN + ERA5 climate normals backfill
  wx_climate_updater/   # Daily NOAA + ERA5 climate update
  wx_forecaster/    # Analog forecast + accuracy tracking
  wx_ml_fitter/     # Weekly logistic regression training
  wx_summarizer/    # Daily prose summaries
  wx_records_tracker/ # Weekly station records
dashboard/
  index.html        # Dashboard UI
  app.js            # Fetch + render logic
  style.css         # Styles
  docs.html         # How It Works
cdk/
  wx_stack.py       # CDK stack (all infrastructure)
  app.py            # CDK entry point
```

---

## Deployment

```bash
# Deploy infrastructure
cd cdk && npx cdk deploy --require-approval never

# Deploy dashboard
aws s3 sync dashboard/ s3://wx-jamestannahill-dashboard/ --cache-control no-cache
aws cloudfront create-invalidation --distribution-id E2OIRPWQ2L8LB6 --paths "/*"
```

Estimated AWS cost: **~$4–6/month**.

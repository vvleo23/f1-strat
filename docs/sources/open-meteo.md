# Open-Meteo

**Status:** Historical ECMWF IFS Single-Run selection and Hungary 2026 persistence implemented; scheduled future capture remains planned.

[Open-Meteo](https://open-meteo.com/) provides global numerical weather-model data through consistent HTTP APIs. This project uses it for immutable point-in-time forecasts. OpenF1 and FastF1 weather remain separate session observations and evaluation references.

## API choice

| API | Project use | Decision |
|---|---|---|
| [Weather Forecast](https://open-meteo.com/en/docs) | Capture forecasts during a future selected weekend | Primary scheduled snapshot source |
| [Single Runs](https://open-meteo.com/en/docs/single-runs-api) | Reproduce one explicit historical model run and its full forecast horizon | Primary historical replay source when the run was available |
| [Historical Forecast](https://open-meteo.com/en/docs/historical-forecast-api) | Continuous series stitched from the first hours of successive runs | Training, benchmark, and forecast evaluation only |
| [Previous Runs](https://open-meteo.com/en/docs/previous-runs-api) | Fixed lead-time series from previous runs | Optional lead-time comparison such as `T-1 day` |
| [Historical Weather](https://open-meteo.com/en/docs/historical-weather-api) | Retrospective model-based weather | Evaluation reference only, never a forecast input |

The Single Runs `run` parameter is the model initialization time, not its public availability. Global models commonly need another four to six hours and regional models one to three hours before distribution. The pipeline therefore stores both `run_initialized_at` and `available_at`. A run is valid for a replay state only when `available_at <= decision_time`.

## Persisted weekend pipeline data

- air temperature and relative humidity
- precipitation and rain
- cloud cover and weather code
- wind speed and direction
- surface pressure
- model, reviewed Wikidata circuit coordinates, elevation, request parameters, and units
- `snapshot_id`, `run_initialized_at`, `available_at`, `retrieved_at`, `valid_time`, and `lead_time_minutes`
- raw response path, content hash, status, and error context

## Snapshot jobs

The Hungary acceptance run loads the ECMWF IFS run initialized at `2026-07-26T00:00:00Z`. The pipeline stores 168 hourly rows, the immutable raw response, normalized Silver facts, hashes, and the exact request. When explicit run times are omitted, it selects the latest 00/06/12/18 UTC model cycle whose initialization plus a conservative six-hour publication allowance is not later than `decision_time`. This policy is not proof of observed historical availability. The returned forecast horizon must cover the selected target session.

- Store Weather Forecast API responses at approximately `T-24 h`, `T-6 h`, and `T-1 h` for each relevant session.
- Read the validated WGS84 circuit point from curated Wikidata master data; never geocode a circuit or city name inside the weather job.
- Batch required variables and locations to stay within provider limits.
- Write each response atomically and never replace an earlier valid snapshot.
- For historical replay, select the latest proven available Single Run at `decision_time`.
- Keep forecast values separate from OpenF1 and FastF1 observations when joining by `valid_time`.

## Verification

- Confirm bounded connectivity and the expected time arrays, variables, units, coordinates, and model.
- Verify that `run_initialized_at`, `available_at`, `retrieved_at`, and `valid_time` are timezone-aware UTC values.
- Do not infer availability from an old initialization or validity timestamp.
- Persist the exact request, raw response, row count, content hash, and read-after-write result.
- Preserve missing values and report `partial`, `stale`, or `unavailable` without stopping F1 ingestion.
- Compare forecasts with session observations only in evaluation outputs; never rewrite the original forecast.

## Coverage and limits

- Global models support Formula 1 circuits worldwide; resolution and available variables depend on the selected model and region.
- Hourly data is the portable worldwide baseline.
- Native 15-minute data is mainly provided by HRRR in North America and ICON-D2 or AROME in Central Europe. Other regions use interpolation from hourly values.
- Grid forecasts are not trackside measurements and must not be described as refined by later observations.
- Single Runs currently archive ECMWF IFS HRES from March 2024 and most other models from 2 April 2026.
- Historical Forecast coverage starts around 2022 and combines successive runs rather than preserving one complete run.
- The free non-commercial endpoint is limited to 10,000 calls per day and has no uptime guarantee.

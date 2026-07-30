# FastF1

**Status:** Partly implemented as a Hungary 2026 race cross-check; weekend session loading is planned.

[FastF1](https://docs.fastf1.dev/) is a Python library for historical Formula 1 timing, telemetry, weather, and session data. It exposes pandas-compatible objects and uses the local `cache/` directory.

## Use in this project

| Data | Relevant fields | Use |
|---|---|---|
| Laps | `Driver`, `LapNumber`, `LapTime`, `Stint`, `Compound`, `TyreLife` | Pace, tyre analysis, and OpenF1 cross-check |
| Validity | `Deleted`, `DeletedReason`, `IsAccurate` | Valid-lap filtering |
| Telemetry | `Distance`, `Speed`, `Throttle`, `Brake`, `nGear`, `RPM` | Later driver and car analysis |
| Weather | temperature, humidity, pressure, rain, and wind | Observed session-weather cross-check |

FastF1 is not the replay timeline and is not required for OpenF1 race-control state. The target pipeline loads matching completed practice, qualifying, sprint, or race sessions only when a selected job profile needs them. Overlapping rows remain separately identifiable and are not counted twice.

## Access

- Historical on-demand loads through the Python library
- No application authentication for the current use case
- FastF1 HTTP cache under `cache/`
- Verified laps and weather stored as raw Parquet under `data/raw/`

## Verification

The session verifier currently resolves the Hungary 2026 event, loads the race without telemetry, and requires non-empty lap data with `Driver`, `LapNumber`, and `LapTime`. Laps are written atomically. Weather is stored when available; missing weather changes the source result to `partial` without discarding valid laps.

The first Gold comparison aggregates OpenF1 and FastF1 separately by driver. It is a plausibility check, not field-level reconciliation.

## Limits

- Newly completed sessions may not be available immediately.
- Coverage and columns vary between seasons and sessions.
- Data loaded after a completed race does not prove that it was available at an earlier simulated decision time.
- Full-session laps, telemetry, tyre, and weather data remain cross-check or evaluation data unless the replay explicitly releases an observation by `decision_time`.
- A missing `LapTime` does not prove deletion; use the explicit deletion fields where available.
- FastF1 weather is observed session context, not a replacement for a point-in-time forecast.
- Missing telemetry remains unknown and must not become zero.
- `Distance` and `RelativeDistance` are lap-relative, not geographic coordinates.
- Store session identity, retrieval time, library version, and original columns for reproducibility.

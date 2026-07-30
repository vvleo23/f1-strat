# f1-strat

Automated Python data pipeline for collecting, validating, aligning, and replaying public Formula 1 race-weekend data.

This `README.md` is the **Single Source of Truth** for project scope, architecture, data model, current status, and usage. Update the status and roadmap here whenever the implementation changes.

## Project goal

The main product is a reliable data pipeline for one selected Formula 1 race weekend. It discovers the meeting and its sessions, runs bounded ingestion jobs, preserves immutable source snapshots, normalizes them, and records their quality and lineage. Historical replay, calculations, predictions, and a later dashboard consume this data; they do not own ingestion.

Free live OpenF1 data is not assumed. The MVP loads historical data after a session becomes available and simulates live arrival through event-time replay. Full historical race data may exist on disk, but calculations may only access the data released by the replay up to `decision_time`.

The current acceptance case remains the 2026 Hungarian Grand Prix race at the Hungaroring, OpenF1 `session_key=11342`. Belgium 2026 (`session_key=11334`) remains a replay regression case, and Zandvoort 2025 (`session_key=9920`) verifies reusable geometry.

## Feature overview

| Capability | Intended output | Phase | Current status |
|---|---|---|---|
| Race-weekend pipeline | Selected meeting, discovered sessions, snapshots, manifests, retries, and finalization | Pipeline MVP | Partly implemented |
| Historical event replay | Chronological race state with a strict future-data boundary | Pipeline MVP | Partly point-in-time-conformant |
| Weather pipeline | Immutable Open-Meteo forecasts plus separate OpenF1/FastF1 observations | Pipeline MVP | Planned |
| Season overview | Calendar, sessions, driver/team standings, wins, and podium counts | After pipeline MVP | Planned |
| Qualifying calculation | Predicted positions and Top-10/Top-3 probabilities from completed weekend sessions | Later analysis | Planned |
| Race calculation | Updated outcome and strategy scenarios from the current replay state | Later analysis | Planned |
| Dashboard | Small read-only view of curated data, replay, weather, and calculation history | Later presentation | Planned |

Circle of Doom, rain radar, pit-window visualization, live streaming, and complex strategy optimization are not Pipeline-MVP requirements. Existing Circle-of-Doom code remains a regression and research artifact.

## Weekend loading policy

Selecting a race weekend always loads meeting and session metadata first. Data jobs then use an explicit purpose instead of downloading every high-volume endpoint blindly:

- **Replay:** load the selected completed race and its required replay endpoints.
- **Qualifying calculation:** load only practice or sprint-practice sessions completed before `decision_time`.
- **Race calculation:** load completed practice, qualifying, sprint, and race observations available before `decision_time`.
- **Cross-check:** load matching FastF1 sessions when available without blocking OpenF1 processing.

Sprint and changed weekend formats are discovered from session metadata; session names and counts are not hard-coded. Training data is therefore loaded automatically when a selected calculation requires it, but not for unrelated calendar or replay requests.

## Current status

Last updated: **25 August 2026**

### Implemented

- Hungary 2026 calendar and race-session discovery
- isolated OpenF1 endpoint verification with cached fallback and source status
- FastF1 race-lap and observed-weather cross-check
- atomic raw Parquet snapshots and OpenF1 season master-data ingestion
- schema, key, timestamp, status, foreign-key, and read-after-write validation
- normalized `season`, `meeting`, `session`, `country`, `circuit`, `circuit_geometry`, `driver`, and `team` dimensions
- session-local centerlines for Hungary 2026 and Zandvoort 2025
- Circle-of-Doom replay with synthetic-circle default and optional stored geometry
- OpenF1 pace-by-stint analysis with a separate FastF1 driver-level comparison
- focused unit tests for validation, master data, geometry, pace, replay, and session selection

### Partly implemented

- Bronze, Silver, and Gold data flow exists, but session facts are not normalized into Silver yet.
- OpenF1 and FastF1 are compared at aggregate driver level, not reconciled field by field.
- The race is verified; practice and qualifying are not included in the reference workflow.
- Track centerlines are local OpenF1 display geometry, not geographic map geometry.
- Race replay already uses backward as-of joins, but there is no reusable central `decision_time` data-cut service yet.
- The replay is not yet safe as a prediction boundary: full-race reference pace, complete stint records, and full-lap path normalization can expose future information.

### Not implemented

- central orchestrator and scheduler
- canonical Raw-to-Curated event transformation
- complete weekend session ingestion and job manifests
- Wikidata circuit-coordinate and Open-Meteo adapters; RainViewer remains deferred
- standings, qualifying calculations, race calculations, and strategy recommendations
- read-only dashboard and paid live ingestion

## Repository structure

```text
f1-strat/
├── README.md                    # Single Source of Truth
├── docs/
│   ├── projektdokumentation.md # German project report
│   ├── sources/                # One English card per external source
│   └── assets/                 # Figures used by the project report
├── src/f1_pipeline/
│   ├── analysis/               # Derived analyses
│   ├── replay/                 # Point-in-time replay
│   ├── sources/                # Source verification and ingestion
│   ├── data_validation.py
│   ├── geometry.py
│   ├── geometry_preview.py
│   └── master_data.py
├── tests/unit/                  # Focused domain tests
├── data/
│   ├── raw/                     # Bronze source snapshots
│   ├── curated/                 # Silver dimensions and future facts
│   └── artifacts/               # Gold reports, analyses, and HTML
└── cache/                       # FastF1 HTTP cache
```

`data/` outputs and `cache/` are local, generated working data and are excluded from Git.

## Pipeline

```text
select meeting
      |
      v
discover sessions and required job profile
      |
      v
OpenF1 / FastF1 / Open-Meteo
      |
      v
data/raw/        Bronze: immutable source evidence and request metadata
      |
      v
validation and normalization
      |
      v
data/curated/    Silver: typed dimensions, facts, events, and forecasts
      |
      +----> decision-time replay ----> calculation snapshots
      |
      v
data/artifacts/  Gold: verification, analyses, replays, and later UI data
```

The workflow is currently started through individual Python modules. The target orchestrator runs idempotent, restartable jobs for a stable meeting or session key. There is no scheduler yet. Parquet is the authoritative project store; the SQLite file in `cache/` belongs only to FastF1. A later DuckDB integration may provide read-only queries over Parquet but must not become a second source of truth.

Failed or incomplete responses never replace the last valid snapshot. Every source or feature uses `available`, `partial`, `stale`, or `unavailable`; missing data remains missing.

## Jobs and target schedule

| Trigger | Job | Result |
|---|---|---|
| Event selection | Discover meeting and all advertised sessions | Stable keys and planned session schedule |
| Daily or schedule change | Refresh calendar and session metadata | Updated or superseded schedule records |
| `T-24 h`, `T-6 h`, `T-1 h` before a session | Store Open-Meteo forecast snapshot | Immutable forecast known before the session |
| About `T+30 min` after session end | Attempt historical OpenF1 ingestion | Raw endpoint snapshots and endpoint status |
| `T+4 h` when data is partial | Retry only missing or partial endpoints | Completed snapshot or retained partial status |
| `T+1 day` | Load FastF1 cross-check and finalize | Curated data, manifest, verification, and artifacts |

These are target defaults, not assumptions that every provider publishes on time. Jobs use bounded retries and can be rerun manually. No job polls forever or requires the application to stay online.

## Data model

The current Silver model contains eight dimensions:

```text
season 1---n meeting 1---n session
meeting n---1 country
meeting n---1 circuit 1---n circuit_geometry
season 1---n driver n---1 team
```

Source identities remain visible in stable IDs such as `openf1:session:11342`. Current session data stays source-shaped in Bronze:

- OpenF1: laps, intervals, positions, locations, pit stops, stints, weather, and race control
- FastF1: laps and observed weather

Planned Silver facts cover laps, intervals, positions, pit stops, stints, race-control events, weather observations, and forecast snapshots. Standings and results remain source-identifiable derived facts rather than fields added to driver or team dimensions.

The planned `circuit` dimension also retains a reviewed Wikidata entity ID, WGS84 latitude and longitude, coordinate source revision, retrieval time, and verification status. These coordinates are weather reference points; the existing OpenF1 centerline remains separate display geometry.

The planned canonical event envelope contains `event_id`, `event_type`, `event_time`, `available_at`, `ingested_at`, `source_system`, `entity_id`, and `schema_version`. Weather forecasts additionally retain `run_initialized_at`, `valid_time`, model, coordinates, request parameters, and units.

Each calculation snapshot stores `calculation_id`, `session_id`, `decision_time`, `trigger_id`, `trigger_type`, `calculated_at`, calculation and feature versions, input-manifest hash, status, and output reference. The name "calculation" is deliberate: a transparent baseline, statistical model, simulation, or later ML model can implement the same contract.

## Replay and calculation contract

```text
complete historical source snapshots
                |
                v
decision-time data cut
                |
                v
RaceState at t -> features at t -> calculation -> immutable result
                |
                v
next chronological trigger
```

- Race observations are released by their source event timestamp; later race observations remain invisible.
- A forecast is visible only when `available_at <= decision_time` and is evaluated for its separate `valid_time`.
- `run_initialized_at` is not proof of availability because weather models require computation and distribution time.
- Recalculation triggers are race start, lap completion, Race Control state changes, pit stops, and newly available forecast snapshots.
- Trigger IDs make repeated processing idempotent. Late data creates a new version; it does not silently rewrite an earlier result.
- Every result retains its input snapshot and versions so the same state can be reproduced.
- A missing minimum input produces `partial` or `unavailable`, not a fabricated prediction.

## Weather policy

- During a future selected weekend, the Weather Forecast API is captured on the target schedule; `retrieved_at` proves when the snapshot was available to this project.
- For historical point-in-time replay, the Single Runs API retrieves one explicit model run. The selected run must have been publicly available by `decision_time`; model initialization alone is too early.
- The Historical Forecast API is useful for continuous training and benchmark series but does not represent one fixed pre-session forecast horizon.
- The Previous Runs API is useful for lead-time comparisons such as `T-1 day`; it is not the primary replay input.
- The Historical Weather API, OpenF1 weather, and FastF1 weather are observations or retrospective references. They may evaluate forecasts and may update a calculation only after their observation time; they never overwrite a forecast snapshot.
- Circuit latitude and longitude come from reviewed Wikidata master data; weather jobs do not guess coordinates from a circuit or city name.
- Open-Meteo provides global model coverage. Hourly data is the worldwide baseline. Native 15-minute data is available mainly in Central Europe and North America; elsewhere it is interpolated and remains optional.
- The non-commercial free endpoint is limited to 10,000 calls per day and has no uptime guarantee. Weekend jobs must batch variables and coordinates and cache every valid response.

## Sources

| Source | Responsibility | Status |
|---|---|---|
| [OpenF1](docs/sources/openf1.md) | Meeting/session discovery, replay events, results, standings, and local vehicle coordinates | Race path implemented |
| [FastF1](docs/sources/fastf1.md) | Matching historical sessions, laps, tyres, telemetry, weather, and cross-checks | Race cross-check implemented |
| [Open-Meteo](docs/sources/open-meteo.md) | Immutable point-in-time forecast runs and forecast evaluation data | Planned |
| [RainViewer](docs/sources/rainviewer.md) | Optional later radar and short-term precipitation nowcasts | Deferred |
| [Wikidata](docs/sources/wikidata.md) | Reviewed WGS84 circuit reference points for weather requests | Planned |

Overlapping source rows are never blindly merged or counted twice. Each feature declares one primary source, while cross-check data remains separately identifiable.

## Presentation boundary

The dashboard is not part of the Pipeline MVP. When the data contracts are stable, the preferred first implementation is Streamlit with Plotly because it matches the Python and Parquet stack. It must remain a small read-only consumer of curated data and artifacts: no source requests, snapshot writes, orchestration, or model fitting inside UI code. A custom frontend or Dash is considered only if replay interaction cannot be delivered simply.

The later overview contains the season calendar and session selector, driver and team standings, wins, podiums, and other small tables. The replay view contains a driver order and position list, the race on the stored track layout, the point-in-time weather forecast, and Race Control events. Circle of Doom, rain radar, pit-window graphics, and complex custom visualizations are excluded from the first dashboard.

## Setup

Python `3.14` is configured in `.python-version`.

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
PYTHONPATH=src python -m unittest discover -s tests/unit -p 'test_*.py'
```

## Usage

Run the Hungary verification from existing snapshots when possible:

```bash
PYTHONPATH=src python -m f1_pipeline.sources.session_verification
```

Fetch current source data and fail unless the required OpenF1 inputs are available:

```bash
PYTHONPATH=src python -m f1_pipeline.sources.session_verification --refresh --strict
```

Load the 2026 season master data:

```bash
PYTHONPATH=src python -m f1_pipeline.master_data --season 2026
```

Build and preview the Hungary centerline:

```bash
PYTHONPATH=src python -m f1_pipeline.geometry --session-key 11342
PYTHONPATH=src python -m f1_pipeline.geometry_preview \
  --session-key 11342 \
  --self-contained \
  --open
```

Create the Hungary replay:

```bash
PYTHONPATH=src python -m f1_pipeline.replay.circle_of_doom \
  --session-key 11342 \
  --driver VER \
  --self-contained \
  --output data/artifacts/circle_of_doom_hungary_2026.html
```

Add `--geometry-mode stored` to render the stored centerline instead of the default synthetic circle.

Build the pace analysis without fetching new data:

```bash
PYTHONPATH=src python -m f1_pipeline.analysis.pace --session-key 11342
```

Generated verification JSON, replay HTML, geometry previews, and analysis files are written below `data/artifacts/`. They are reproducible outputs, not source data.

There is no combined weekend-orchestrator command yet. The commands above describe only the currently implemented paths and must not be presented as the target automated workflow.

## MVP completion criteria

- A meeting selection discovers its current sessions without assuming a fixed weekend format.
- Required sessions and endpoints run through idempotent jobs with manifests and isolated status.
- Forecast snapshots and source responses remain immutable and reproducible.
- Partial publication is retried without replacing successful data or stopping independent jobs.
- One central `decision_time` cut prevents future race and forecast data from entering calculations.
- Calculation snapshots retain trigger, input hash, versions, status, and output.
- The Hungary reference race can be replayed from recorded snapshots with no future leakage.
- Missing values remain missing, and source, curated, and derived data stay separated.

## Roadmap

1. Remove replay leakage from reference pace, lap progress, and stint visibility; verify one strict `decision_time` cut.
2. Implement meeting selection, session discovery, job profiles, and one manual weekend orchestrator.
3. Add reviewed Wikidata circuit coordinates and persist their source evidence and verification status.
4. Add practice, qualifying, sprint, and race ingestion with manifests and no hard-coded weekend formats.
5. Normalize Bronze session data into canonical Silver facts and timeline events.
6. Add Open-Meteo snapshot jobs with `run_initialized_at`, `available_at`, and `valid_time`.
7. Add trigger-driven immutable Calculation Snapshots with input hashes and versions.
8. Add persisted retry scheduling and session finalization.
9. Add results, driver/team standings, wins, and podium summaries as simple derived outputs.
10. Build transparent qualifying and race baselines with temporal backtests before considering ML.
11. Add strategy scenarios and the small read-only dashboard only after pipeline outputs are reliable.

Paid live streaming, private team-data inference, rain radar, and an official steward-decision feed remain outside the current MVP. The existing `pit_exit_projection` is hypothetical and must not be presented as a strategy recommendation.

## Documentation

- [`docs/projektdokumentation.md`](docs/projektdokumentation.md): German project report, process, verification evidence, problems, decisions, and figures
- [`docs/sources/`](docs/sources/): concise English source cards

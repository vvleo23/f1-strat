# f1-strat

Automated Python data pipeline for collecting, validating, aligning, and replaying public Formula 1 race-weekend data.

This `README.md` is the **Single Source of Truth** for project scope, architecture, data model, current status, and usage. Update the status and roadmap here whenever the implementation changes.

## Project goal

The main product is a reliable, season-parameterized data pipeline for any selected Formula 1 race weekend available from OpenF1. It discovers the meeting and its sessions, runs bounded ingestion jobs, preserves immutable source snapshots, normalizes them, and records their quality and lineage. Pipeline and Dashboard V1 add a historical replay and a small read-only UI. Calculation snapshots, predictions, online strategy, and pit-window recommendations are post-V1 consumers; none of these consumers owns ingestion.

Free live OpenF1 data is not assumed. Pipeline and Dashboard V1 load historical data after a session becomes available and simulate live arrival through event-time replay. Full historical race data may exist on disk, but later calculations may only access the data released by the replay up to `decision_time`.

The current acceptance case remains the 2026 Hungarian Grand Prix race at the Hungaroring, OpenF1 `session_key=11342`. Belgium 2026 (`session_key=11334`) remains a replay regression case, and Zandvoort 2025 (`session_key=9920`) verifies reusable geometry.

### System context

```mermaid
flowchart LR
    OpenF1[OpenF1<br/>weekend and race timeline]
    FastF1[FastF1<br/>independent cross-check]
    Wikidata[Wikidata<br/>reviewed or safely auto-verified circuit identity]
    OpenMeteo[Open-Meteo<br/>weather forecasts]

    Pipeline[F1 weekend data pipeline]
    Store[(Parquet snapshots,<br/>curated data, and manifests)]
    Replay[Historical point-in-time replay]
    Analysis[Pace and source analysis]
    Calculations["Calculation snapshots and<br/>strategy recommendations<br/>(planned)"]
    UI["Read-only Streamlit dashboard"]

    OpenF1 --> Pipeline
    FastF1 --> Pipeline
    Wikidata --> Pipeline
    OpenMeteo --> Pipeline
    Pipeline --> Store
    Store --> Replay
    Store --> Analysis
    Replay -.-> Calculations
    Store -.-> Calculations
    Store --> UI
    Calculations -.-> UI
```

## Feature overview

| Capability | Intended output | Phase | Current status |
|---|---|---|---|
| Race-weekend pipeline | Selected meeting, discovered sessions, snapshots, manifests, retries, and finalization | Pipeline MVP | Generic purpose planner, Weekend Complete V1, and controlled job service implemented; scheduling planned |
| Historical event replay | Chronological race state for historical visualization | Pipeline and Dashboard V1 | Implemented with synchronized Re-Live panels and a strict prediction boundary |
| Weather pipeline | Immutable Open-Meteo forecasts plus separate OpenF1/FastF1 observations | Pipeline MVP | Generic circuit-reference path implemented; forecast coverage depends on provider availability |
| Season overview | Calendar, sessions, driver/team standings, wins, and podium counts | Dashboard V1 | Implemented from curated OpenF1 facts |
| Qualifying calculation | Full predicted classification plus per-driver Top-15/Top-10/Top-3 probabilities and teammate comparison | Later analysis | Planned |
| Race calculation | Online strategy and pit-window recommendations from the current replay state | Post-V1 analysis | Planned |
| Dashboard | Small read-only view of curated data, results, standings, replay, weather, and Race Control | Dashboard V1 | Season overview and single-screen Re-Live implemented |

Rain radar is rejected. Qualifying prediction, race prediction, strategy recommendation, and pit-window calculation are not Pipeline and Dashboard V1 requirements. Circle of Doom shows a transparent immediate-stop projection for the selected driver only when a configured circuit average and current green-flag gaps are available. It is an assumption, not a strategy recommendation.

## Weekend loading policy

Selecting a race weekend always loads meeting and session metadata first. Data jobs then use an explicit purpose instead of downloading every high-volume endpoint blindly:

- **Selected session:** load only the selected session's facts; this is the lightweight dashboard default.
- **Replay:** load the selected completed race plus the high-volume location timeline required for replay.
- **Qualifying calculation:** load sessions completed before the target qualifying session and available by `decision_time`; the later feature contract decides which practice, sprint, weather, and tyre inputs are admissible.
- **Race calculation:** load completed practice, qualifying, sprint, and race observations available before `decision_time`.
- **Cross-check:** load matching FastF1 sessions when available without blocking OpenF1 processing.

Sprint and changed weekend formats are discovered from session metadata; session names and counts are not hard-coded. The pipeline can ingest all discovered sessions or select a deterministic subset by repeated OpenF1 session key, canonical session type, or the intersection of both filters. Training data is therefore loaded automatically when a selected calculation requires it, but not for unrelated calendar or replay requests.

The implemented `weekend` profile loads drivers, laps, stints, weather, and the applicable position, pit, interval, Race Control, starting-grid, result, and championship endpoints. The dashboard narrows this profile to the selected session. Practice and qualifying do not request the unavailable `intervals` endpoint. Starting grids are optional for races, results are optional for qualifying, sprint, and race, and championship standings are optional after sprint and race.

`purpose=weekend_complete_v1` without session filters and with a `decision_time` after the last completed session is the dashboard's explicit large complete-weekend action. It additionally persists the OpenF1 `sessions` response for every selected session and high-volume `location` for sprint and race. Location is requested per driver and combined locally to avoid oversized OpenF1 responses. A reviewed or safely auto-verified circuit and one target-session weather forecast remain part of the run. Manifest-bound `sessions`, `laps`, and `location` snapshots automatically produce season-partitioned local track geometry when sufficient data exists. This V1 profile is not an all-source export: it excludes location for practice and qualifying, telemetry, a full matching FastF1 weekend, and multiple scheduled forecast vintages.

The repository includes a 3.6 MiB Hungaroring demo dataset with one race, weather, results, standings, and a downsampled replay timeline. The dashboard uses it automatically when no locally ingested season data exists. Full source snapshots, full-resolution location data, generated artifacts, and caches remain excluded from Git.

## Current status

Last updated: **4 September 2026**

### Implemented

- Hungary 2026 calendar and race-session discovery
- isolated OpenF1 endpoint verification with cached fallback and source status
- FastF1 race-lap and observed-weather cross-check
- atomic raw Parquet snapshots and OpenF1 season master-data ingestion
- schema, key, timestamp, status, foreign-key, and read-after-write validation
- normalized `season`, `meeting`, `session`, `country`, `circuit`, `circuit_geometry`, `driver`, and `team` dimensions
- versioned reviewed-circuit registry plus a separate locked auto-verified registry for previously unknown OpenF1 circuits
- immutable Wikidata evidence and an ECMWF IFS Open-Meteo Single Run for Hungary 2026
- F1-Wikidata-Open-Meteo weekend pipeline with an idempotent schema-v6 run manifest
- complete Hungary 2026 ingestion for Practice 1, Practice 2, Practice 3, Qualifying, and Race
- validated full-weekend or selective ingestion by session key and canonical session type
- purpose-based `weekend`, `weekend_complete_v1`, `replay`, `qualifying_prediction`, and `race_strategy` session planning at an explicit `decision_time`
- season-partitioned authoritative master dimensions with temporary 2026 legacy aliases
- one shared OpenF1 HTTP transport and Bronze cache-path convention for master data, weekend ingestion, replay, and verification
- automatic conservative selection of the latest historical ECMWF model cycle available at `decision_time`
- central point-in-time weather cut selecting one available forecast vintage and only already available track observations
- reusable strict fact cut requiring both `event_time <= decision_time` and `available_at <= decision_time`
- immutable OpenF1 endpoint snapshots, session manifests, and a combined weekend-facts manifest
- canonical Silver `session_entry`, `lap`, `interval`, `position`, `pit_stop`, `stint`, `race_control_event`, `weather_observation`, `starting_grid`, `session_result`, `driver_championship_standing`, and `team_championship_standing` facts
- read-only Streamlit season overview and session replay backed by manifest-selected, hash-verified curated data
- separate local HTTP job service with deterministic intents and persisted status for selected-session and Weekend Complete V1 actions
- automatic manifest-bound local centerlines in season-partitioned geometry dimensions
- leakage-free Circle-of-Doom replay with point-in-time reference pace, causal lap progress, released stint visibility, synthetic-circle default, and optional stored geometry
- OpenF1 pace-by-stint analysis with a separate FastF1 driver-level comparison
- focused unit tests for validation, master data, geometry, pace, replay, session selection, result/standing ingestion, and job execution
- bundled hash-verified Hungaroring demo data with automatic dashboard fallback
- per-driver OpenF1 location ingestion for bounded high-volume requests
- unified m/s wind-speed display and hourly-to-target linear interpolation (circular for wind direction) between the current track observation and the Open-Meteo forecast tiles in Re-Live
- weekend-level forecast summary, interpolated per session start, in the season-overview meeting dialog
- locking around the shared season-wide master dimensions and the shared circuit dimension so concurrent weekend jobs for different meetings can no longer silently discard each other's writes; the `starting_grid` OpenF1 endpoint's permanent HTTP 404 is now treated as a legitimate empty response instead of an ingestion failure

### Partly implemented

- OpenF1 and FastF1 are compared at aggregate driver level, not reconciled field by field.
- Unknown circuits are accepted automatically only when exactly one Wikidata candidate passes circuit/ location, country, racing-description, Earth-coordinate, and revision checks; ambiguous or inconsistent source metadata remains `partial` and requires review.
- The orchestrator and local job runner are restartable and idempotent; delayed retry scheduling and automatic finalization are not implemented.
- Sprint session planning is implemented and unit-tested, but the current Hungary acceptance weekend has no Sprint.
- Track centerlines are local OpenF1 display geometry, not geographic map geometry.
- The new cross-meeting lock covers jobs started through the weekend-weather pipeline (dashboard and CLI weekend runs); a concurrent direct `python -m f1_pipeline.master_data` CLI call is not covered. A waiting job also redoes the full season master-data rebuild after acquiring the lock rather than reusing the winner's result; accepted for a single-user tool rather than optimized away.
- `OpenF1Client` rate-limits requests per instance, not per season or globally, so several concurrent jobs can still add up to a higher combined request rate than intended; not fixed for now (see decision log — deprioritized as a scaling concern for a private, single-user tool).
- A single historical HTTP 401 from the OpenF1 `meetings` endpoint was observed with no retry budget (401 is not in the retry status list); not reproduced since and not fixed for now.

### Not implemented

- delayed retry scheduler and automatic session finalization
- qualifying calculations, race calculations, and strategy recommendations
- paid live ingestion
- a shared rate limit across concurrent OpenF1 jobs and a retry budget for transient HTTP 401 responses (see "Partly implemented"; deprioritized for a private, single-user tool)

## Repository structure

```text
f1-strat/
├── README.md                    # Single Source of Truth
├── demo_data/                   # Small bundled dashboard and replay dataset
├── config/
│   ├── reviewed_circuit_mappings.json # Reviewed OpenF1-to-Wikidata identities
│   └── pit_loss_seconds.json          # Editable circuit-average pit losses
├── docs/
│   ├── projektdokumentation.md # German project report
│   ├── sources/                # One English card per external source
│   └── assets/                 # Figures used by the project report
├── src/f1_pipeline/
│   ├── analysis/               # Derived analyses
│   ├── replay/                 # Point-in-time replay
│   ├── sources/                # Source verification and ingestion
│   │   └── openf1.py           # Shared OpenF1 transport and Bronze path conventions
│   ├── data_validation.py
│   ├── planning.py              # Purpose and decision-time session planning
│   ├── persistence.py          # Atomic file persistence and hashing
│   ├── session_facts.py        # Canonical OpenF1 Silver facts
│   ├── temporal.py             # Shared strict decision-time fact cut
│   ├── demo_data.py            # Rebuild the bundled demo from local snapshots
│   ├── job_runner.py           # Deterministic persisted weekend jobs
│   ├── job_service.py          # Controlled HTTP boundary for dashboard jobs
│   ├── dashboard/              # Read-only Streamlit views and read models
│   ├── weather.py              # Point-in-time forecasts and track observations
│   ├── geometry.py
│   ├── geometry_preview.py
│   └── master_data.py
├── tests/unit/                  # Focused domain tests
├── data/
│   ├── raw/                     # Bronze source snapshots
│   ├── curated/                 # Silver dimensions and future facts
│   │   └── registries/          # Persisted auto-verified Wikidata identities
│   └── artifacts/               # Gold reports, analyses, and HTML
└── cache/                       # FastF1 HTTP cache
```

`data/` outputs and `cache/` are local, generated working data and are excluded from Git.

## Pipeline

### Architecture and data zones

```mermaid
flowchart LR
    subgraph Sources[External sources]
        OF1[OpenF1]
        FF1[FastF1]
        WD[Wikidata]
        OM[Open-Meteo]
    end

    subgraph PipelineServices[Pipeline services]
        Select[Meeting selection and<br/>purpose-based session planning]
        Ingest[Bounded source ingestion]
        Validate[Validation and normalization]
        Persist[Atomic persistence and hashing]
    end

    subgraph Bronze["Bronze — data/raw"]
        Raw[Immutable source snapshots<br/>and request evidence]
    end

    subgraph Silver["Silver — data/curated"]
        Dimensions[Typed dimensions]
        Facts[Temporal facts and forecasts]
        Manifests[Run and lineage manifests]
    end

    subgraph Consumers[Point-in-time consumers]
        Cut[Decision-time data cut]
        RaceState[Race State and features]
        Strategy["Calculation snapshots and<br/>strategy service — planned"]
    end

    subgraph Gold["Gold — data/artifacts"]
        Verification[Verification reports]
        Analyses[Analyses and replays]
    end

    subgraph Applications[Applications]
        Presentation["Read-only Dashboard V1 — implemented"]
    end

    Sources --> Select
    Select --> Ingest
    Ingest --> Raw
    Raw --> Validate
    Validate --> Persist
    Persist --> Dimensions
    Persist --> Facts
    Persist --> Manifests
    Dimensions --> Cut
    Facts --> Cut
    Manifests --> Cut
    Cut --> RaceState
    RaceState -.-> Strategy
    Dimensions --> Verification
    Facts --> Verification
    Manifests --> Verification
    Facts --> Analyses
    RaceState --> Analyses
    RaceState --> Presentation
    Dimensions --> Presentation
    Facts --> Presentation
    Manifests --> Presentation
    Strategy -.-> Presentation
    Analyses -.-> Presentation
```

The weekend pipeline runs OpenF1 master-data selection, purpose- and decision-time-based session planning, session-fact ingestion, Silver normalization, reviewed or safely auto-verified Wikidata coordinates, Open-Meteo normalization, automatic local geometry, and a final content-identified manifest. Purpose, target session, session filters, resolved session keys, both registry provenances, geometry lineage, and source hashes are part of the schema-v6 manifest and run identity. Master dimensions and geometry are authoritative below `data/curated/dimensions/season=<year>/`; unpartitioned 2026 files remain temporary compatibility aliases. Ambiguous Wikidata candidates, unavailable weather, and unusable geometry fail independently and preserve successful OpenF1 facts. Persisted retry scheduling and automatic finalization are not implemented yet. Parquet is the authoritative project store; the SQLite file in `cache/` belongs only to FastF1. A later DuckDB integration may provide read-only queries over Parquet but must not become a second source of truth.

Failed or incomplete responses never replace the last valid snapshot. Every source or feature uses `available`, `partial`, `stale`, or `unavailable`; missing data remains missing.

### FastF1 verification cross-check

FastF1 is currently a separate Hungary 2026 race cross-check, not part of the generic weekend orchestrator. Its failure does not block the primary OpenF1 path, and telemetry is explicitly disabled.

```mermaid
sequenceDiagram
    actor Operator
    participant Verify as Session verifier
    participant FastF1
    participant Cache as FastF1 HTTP cache
    participant Raw as data/raw
    participant Artifacts as data/artifacts
    participant Analysis as Separate pace analysis

    Operator->>Verify: Run Hungary source verification
    Verify->>Cache: Enable local HTTP cache
    Verify->>FastF1: Load 2026 calendar and Hungary Race
    Note over Verify,FastF1: telemetry=False, weather=True
    FastF1-->>Verify: Race laps and optional observed weather
    Verify->>Verify: Validate required lap fields
    Verify->>Raw: Persist FastF1 laps atomically
    opt Weather is available
        Verify->>Raw: Persist observed weather atomically
    end
    Verify->>Artifacts: Write independent source status

    opt Separate analysis command
        Operator->>Analysis: Build pace comparison
        Analysis->>Raw: Read OpenF1 and FastF1 laps separately
        Analysis->>Analysis: Aggregate by driver and compare medians
        Analysis->>Artifacts: Write comparison Parquet and metadata
    end
```

The cross-check currently covers one race, laps, and optional observed weather. It does not load telemetry, ingest a full matching FastF1 weekend, reconcile individual rows with OpenF1, or participate in Dashboard V1 jobs.

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

```mermaid
erDiagram
    SEASON ||--o{ MEETING : contains
    SEASON ||--o{ DRIVER : registers
    SEASON ||--o{ TEAM : registers
    COUNTRY ||--o{ MEETING : hosts
    CIRCUIT ||--o{ MEETING : hosts
    CIRCUIT ||--o{ CIRCUIT_GEOMETRY : has
    MEETING ||--o{ SESSION : contains

    SESSION ||--o{ SESSION_ENTRY : has
    DRIVER ||--o{ SESSION_ENTRY : participates
    TEAM ||--o{ SESSION_ENTRY : enters

    SESSION ||--o{ LAP : records
    SESSION ||--o{ INTERVAL : records
    SESSION ||--o{ POSITION : records
    SESSION ||--o{ PIT_STOP : records
    SESSION ||--o{ STINT : records
    SESSION ||--o{ RACE_CONTROL_EVENT : records
    SESSION ||--o{ WEATHER_OBSERVATION : records
    SESSION ||--o{ WEATHER_FORECAST : targets
    SESSION ||--o{ SESSION_RESULT : records
    SESSION ||--o{ DRIVER_CHAMPIONSHIP_STANDING : records
    SESSION ||--o{ TEAM_CHAMPIONSHIP_STANDING : records
    CIRCUIT ||--o{ WEATHER_FORECAST : locates

    SESSION_ENTRY ||--o{ LAP : completes
    SESSION_ENTRY ||--o{ INTERVAL : has
    SESSION_ENTRY ||--o{ POSITION : has
    SESSION_ENTRY ||--o{ PIT_STOP : makes
    SESSION_ENTRY ||--o{ STINT : completes
    SESSION_ENTRY ||--o{ SESSION_RESULT : receives
    DRIVER ||--o{ DRIVER_CHAMPIONSHIP_STANDING : ranks
    TEAM ||--o{ TEAM_CHAMPIONSHIP_STANDING : ranks
```

Source identities remain visible in stable IDs such as `openf1:session:11342`. Current session data stays source-shaped in Bronze:

- OpenF1: laps, intervals, positions, locations, pit stops, stints, weather, race control, starting grids, session results, and championship standings
- FastF1: laps and observed weather

Implemented Silver facts cover session entries, laps, intervals, positions, pit stops, stints, Race Control events, weather observations, forecast snapshots, starting grids, session results, driver championship standings, and team championship standings. Standings and results remain source-identifiable facts rather than fields added to driver or team dimensions.

The `circuit` dimension retains a Wikidata entity ID, WGS84 latitude and longitude, coordinate revision, retrieval time, raw evidence, hash, and verification status. Manually reviewed identities come from `config/reviewed_circuit_mappings.json` and always take precedence. Unknown circuits trigger one bounded name search and entity retrieval. Exactly one candidate must match the normalized OpenF1 circuit name or location, country, racing-circuit description, one non-deprecated Earth `P625`, and valid coordinate ranges. It is then atomically written to `data/curated/registries/auto_wikidata_circuit_mappings.json`; search and entity evidence remain immutable. Multiple or invalid candidates remain `partial` and never supply weather coordinates. Both registry hashes enter the run identity. Wikidata coordinates are weather reference points; OpenF1 local centerlines remain separate display geometry.

The canonical fact envelope contains a deterministic `fact_id`, fact type, session and optional session-entry identity, `event_time`, `available_at`, `ingested_at`, `availability_basis`, source record identity, raw path, raw hash, and schema version. Historical OpenF1 facts use `simulated_event_time`; rows without a derivable event time remain explicitly unavailable. Completed laps and stints become visible at the derived lap end, not at lap start. The weather-forecast fact separately retains `snapshot_id`, session and circuit IDs, `run_initialized_at`, `available_at`, `retrieved_at`, `decision_time`, `valid_time`, model, reviewed coordinates, grid coordinates, request evidence, values, units, and hashes.

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
- Weather consumers use `weather.build_weather_cut` so later forecasts and track observations cannot enter an earlier calculation.
- Silver-fact consumers use `temporal.cut_facts`; malformed or missing temporal values remain invisible.
- Replay reference pace is estimated per frame from completed laps, lap progress uses only prior completed-lap distance, and complete stints remain hidden until their availability time.
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
- Circuit latitude and longitude come from reviewed or uniquely auto-verified Wikidata master data; weather jobs never guess coordinates from a circuit or city name.
- Open-Meteo provides global model coverage. Hourly data is the worldwide baseline. Native 15-minute data is available mainly in Central Europe and North America; elsewhere it is interpolated and remains optional.
- The non-commercial free endpoint is limited to 10,000 calls per day and has no uptime guarantee. Weekend jobs must batch variables and coordinates and cache every valid response.

## Sources

| Source | Responsibility | Status |
|---|---|---|
| [OpenF1](docs/sources/openf1.md) | Meeting/session discovery, replay events, local vehicle coordinates, results, and standings | Weekend Complete V1 and replay paths implemented |
| [FastF1](docs/sources/fastf1.md) | Race laps, tyres, observed weather, and independent comparison; telemetry and weekend loading are planned | Hungary race cross-check implemented |
| [Open-Meteo](docs/sources/open-meteo.md) | Immutable point-in-time forecast runs and forecast evaluation data | Hungary Single Run implemented |
| [Wikidata](docs/sources/wikidata.md) | Reviewed or uniquely auto-verified WGS84 circuit reference points for weather requests | Generic fail-closed resolution implemented |

Overlapping source rows are never blindly merged or counted twice. Each feature declares one primary source, while cross-check data remains separately identifiable.

### Bronze boundary and source-to-feature traceability

The provider API or library is outside the project data lake. **Bronze, or Stage 1, starts when the project persists the source-shaped response together with request, retrieval, identity, status, and hash evidence.** OpenF1 and FastF1 tabular responses are stored as raw Parquet, while Open-Meteo and Wikidata evidence is stored as immutable JSON. The FastF1 SQLite file is only an HTTP cache. Normalized facts, dimensions, and the locally derived circuit centerline are Silver rather than raw source data.

| Source | Persisted Bronze / Stage 1 | Consumer or feature | Role and state |
|---|---|---|---|
| OpenF1 `meetings`, `sessions` | Event keys, names, types, scheduled start/end times | Season/weekend/session selection, planner, replay boundary | Primary; backend and dashboard implemented |
| OpenF1 `drivers` | Driver number, acronym, team, colour, session identity | Driver/team dimensions, session entries, replay labels | Primary; implemented |
| OpenF1 `laps`, `stints` | Lap and sector timing, stint, compound and tyre age | Pace analysis, replay; later qualifying and strategy features | Primary; ingestion implemented, calculations planned |
| OpenF1 `intervals`, `position`, `pit` | Gaps, order, position changes and pit stops | Re-live state and later field-aware strategy | Primary; implemented for applicable session types |
| OpenF1 `race_control` | Timestamped flags, categories and messages | Re-live event feed and track-status triggers | Primary; facts and replay panel implemented |
| OpenF1 `weather` | Timestamped track observations | Weather cut, forecast evaluation and later re-live context | Primary observation; implemented |
| OpenF1 `location` | Timestamped local `x/y/z` vehicle coordinates | Circle/track replay progress and season-partitioned Silver local centerline | Primary display input; automatic for ingested sprint/race sessions |
| OpenF1 `starting_grid`, `session_result`, `championship_drivers`, `championship_teams` | Grid, classification, points, gaps and championship ranks | Grid-to-finish changes, session results, latest standings, wins and podium summaries | Primary; Bronze and Silver persistence implemented |
| FastF1 laps, tyres and weather | Hungary race laps and observed weather Parquet | Separate driver-level pace and source cross-check | Cross-check; partly implemented |
| FastF1 telemetry | Not loaded; telemetry is currently disabled | Possible later speed, throttle, brake, gear and RPM analysis | Planned, with no current consumer |
| Open-Meteo Single Run | Model run, hourly values, units, grid and request evidence | Point-in-time forecast for replay, qualifying and strategy | Primary forecast; Hungary implemented, wider capture planned |
| Wikidata search, entity and `P625` | Search/entity responses, source revision and WGS84 point | Reviewed or uniquely auto-verified circuit identity and Open-Meteo request coordinate | Supporting master source; generic fail-closed resolution implemented |

OpenF1 `location.x/y/z` is not a geographic track map. The stored track view is a Silver centerline derived from vehicle paths. Wikidata supplies one independent WGS84 weather reference point, not the track shape. No source currently supplies persisted project telemetry.

## Presentation boundary

Dashboard V1 uses Streamlit, Plotly for overview charts, and an isolated Streamlit Components v2 renderer for synchronized Re-Live playback. It remains a small read-only consumer of curated data and artifacts: no source requests, snapshot writes, orchestration, or model fitting occur inside UI code. Data actions submit intents to the separate local job service. Calculation Snapshots and strategy outputs will be added only after their pipeline services exist.

The Analysis Dashboard is the single entry point. It shows every race weekend in the selected season as a calendar card with its discovered practice, sprint, qualifying, and race sessions. Manifest-backed states distinguish locally loaded, not-yet-loaded, and not-yet-available sessions. Selecting a Grand Prix opens a local weekend overview with the data-derived season round, compact session/coordinate/weather states, a stored-track preview when present, qualifying and race result tables when available, and a complete-weekend load intent. Qualifying gaps use the last session segment reached by each driver; race gaps preserve seconds, lap deficits, DNF, DNS, and DSQ from the normalized result. Classified race positions also show the change from the qualifying result with green gain, red loss, and grey unchanged indicators; unclassified results remain `NC`. Session dialogs submit lightweight loads, confirm explicit reloads, prepare missing replay data, and require a focus driver before opening Re-Live. The project name is the persistent home control. Driver and constructor standings are displayed side by side below the calendar; the driver table includes numbers, wins, and podiums. Standings progression, teammate comparison, grid-to-finish extremes, and fastest valid stationary stops use only locally loaded facts and expose their race and row coverage. Re-Live fits a 1920×1080 desktop without page scrolling and synchronizes the current order, Circle-of-Doom or stored-track cars, weather observations, session-relevant forecast slots, pit state, and Race Control with one replay clock. Significant Race Control events also appear as a temporary bottom notification. Qualifying Prediction, Race Strategy, pit windows, assumptions, and alternatives remain planned and disabled.

### Dashboard V1 and post-V1 extensions

| Product area | Available in V1 | Post-V1 extensions or operational gaps |
|---|---|---|
| Analysis Dashboard | Read-only season calendar cards, manifest-backed session states, standings, wins and podiums | Calculation history and qualifying/race outputs |
| Year/weekend/session selection | Season control, discovered weekend cards, local weekend and session dialogs, validated CLI filters, and persisted job status | Delayed scheduling and automatic finalization |
| Purpose-based loading | `weekend`, `weekend_complete_v1`, `replay`, `qualifying_prediction`, and `race_strategy` session plans; session-type endpoint profiles | Feature-level endpoint plans across all sources and automatic finalization |
| Complete-weekend state | Red weekend-card frame when all discovered sessions have local data; `weekend_complete_v1` remains available through the service and CLI | Full matching FastF1 weekend, multiple forecast vintages, and retry queue |
| Qualifying calculation | Practice/session laps, stints, compounds and weather inputs | Leakage-free features, baseline/model, full classification, calibrated Top-15/10/3 probabilities and teammate comparison |
| Session Re-Live | Focus-driver selection before entry; synchronized Circle-of-Doom/stored centerline, order, pit state, weather, forecast, Race Control and playback controls | Persisted race-state service and later calculation panels |
| Strategy recommendation | Pit, stint, interval, position, weather and race-state inputs | Versioned algorithm, pit window, alternatives, uncertainty and Calculation Snapshots |

The session-dialog Load Data and Re-Live controls hand deterministic job intents to a separate local HTTP service. Re-loading existing local data requires confirmation and sets `refresh=true`. The dashboard process itself remains read-only: it does not call providers, write snapshots, or execute the pipeline, and it only observes job status, curated data, manifests, and artifacts. The service persists job state but is not a delayed scheduler or distributed queue.

### Dashboard job-control flow

```mermaid
sequenceDiagram
    actor User
    participant UI as Read-only Streamlit dashboard
    participant Client as HTTP job client
    participant Service as Local job service
    participant Runner as Job runner
    participant Status as Job status manifest
    participant Pipeline as Weekend orchestrator
    participant Sources as OpenF1, Wikidata, Open-Meteo
    participant Data as Raw, Silver, and pipeline manifests

    UI->>Data: Read manifest-selected persisted data
    User->>UI: Load session, weekend, or replay data
    UI->>Client: Submit normalized job intent
    Client->>Service: POST /jobs
    Service->>Service: Validate intent and derive deterministic job ID

    alt Identical job is already running
        Service-->>Client: 202 with persisted running status
    else Start or reuse controlled job
        Service->>Runner: Start daemon worker
        Service-->>Client: 202 queued
        Runner->>Runner: Reuse success or acquire exclusive lock
        Runner->>Status: Write running status atomically
        Runner->>Pipeline: Run selected purpose at decision_time
        Pipeline->>Sources: Load or reuse bounded source snapshots
        Pipeline->>Data: Persist snapshots, facts, and manifests
        Data-->>Runner: Pipeline status, run ID, and manifest path
        Runner->>Status: Write final status atomically
        Runner->>Runner: Release lock
    end

    User->>UI: Refresh job status
    UI->>Client: Request job status
    Client->>Service: GET /jobs/{job_id}
    Service->>Status: Read job status manifest
    Status-->>UI: running, available, stale, partial, or unavailable
    UI->>Data: Read newly persisted data through read models
```

The service is a local control plane, not a durable or distributed queue. The dashboard never calls providers or invokes the orchestrator directly; it only submits HTTP intents and reads persisted status and data.

Qualifying is calculated field-wide, without requiring a driver or team selection first, because positions and `Top-N` probabilities depend on the complete entry list. Team and driver selection is a result filter and comparison view, so changing it must not reload source data or rerun the field-wide calculation.

Strategy output is driver-specific and should require a focus driver, optionally selected through a team. Its Race State still contains all competitors for traffic, gaps, pit exit, undercut, and overcut context. With a cached Race State, switching the focus driver reruns only the small strategy calculation, not ingestion or replay construction.

## Setup

Python `3.14` is configured in `.python-version`.

### Windows PowerShell

```powershell
py -3.14 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
$env:PYTHONPATH = "src"
python -m unittest discover -s tests/unit -p "test_*.py"
```

If PowerShell activation is disabled, use `.\.venv\Scripts\python.exe` instead of `python`. PyCharm users can select `.venv\Scripts\python.exe` as the project interpreter and mark `src` as a Sources Root.

### macOS and Linux

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
export PYTHONPATH=src
python -m unittest discover -s tests/unit -p "test_*.py"
```

## Usage

The commands below assume that the virtual environment is active and `PYTHONPATH` is set as shown above.

The dashboard starts with the bundled Hungaroring demo on a fresh clone; no source download is required. Maintainers with the full Hungary snapshots can rebuild the committed demo deterministically:

```bash
python -m f1_pipeline.demo_data
```

Run the Hungary verification from existing snapshots when possible:

```bash
python -m f1_pipeline.sources.session_verification
```

Fetch current source data and fail unless the required OpenF1 inputs are available:

```bash
python -m f1_pipeline.sources.session_verification --refresh --strict
```

Load the 2026 season master data:

```bash
python -m f1_pipeline.master_data --season 2026
```

Run the Hungary Weekend Complete V1 F1-Wikidata-Open-Meteo pipeline from existing snapshots when possible:

```bash
python -m f1_pipeline.sources.weekend_weather_pipeline --season 2026 --meeting-key 1291 --purpose weekend_complete_v1 --target-session-key 11342 --decision-time 2026-07-26T16:00:00Z --run-initialized-at 2026-07-26T00:00:00Z --available-at 2026-07-26T06:00:00Z --strict
```

The historical acceptance case ingests OpenF1 sessions `11335`, `11336`, `11337`, `11338`, and `11342` for meeting `1291`, Wikidata `Q171356`, and the ECMWF IFS run initialized at `2026-07-26T00:00:00Z`. Its `available_at=2026-07-26T06:00:00Z` is a conservative documented-latency policy, not an observed historical retrieval timestamp. Omitting both run arguments deterministically selects the latest 00/06/12/18 UTC cycle whose initialization plus six-hour publication allowance does not exceed `decision_time`. Use `--refresh` only to create new source and manifest versions.

Load only the lightweight Hungary race facts while retaining Race `11342` as the forecast target:

```bash
python -m f1_pipeline.sources.weekend_weather_pipeline --season 2026 --meeting-key 1291 --purpose weekend --target-session-key 11342 --decision-time 2026-07-26T16:00:00Z --include-session-key 11342 --strict
```

Use `--purpose replay` only when the full location timeline is needed.

Load all discovered practice sessions:

```bash
python -m f1_pipeline.sources.weekend_weather_pipeline --season 2026 --meeting-key 1291 --purpose weekend --target-session-key 11342 --decision-time 2026-07-26T16:00:00Z --include-session-type practice --strict
```

Repeat `--include-session-key` or `--include-session-type` to narrow the sessions allowed by the selected purpose. When both filters are present, only their intersection is ingested. Invalid, foreign, cancelled, future, or empty selections are rejected. `--target-session-key` identifies the replay, qualifying, or race-strategy target and the session whose time horizon the forecast must cover. The older `--session-key` and `--forecast-session-key` spellings remain CLI aliases.

Build and preview the Hungary centerline:

```bash
python -m f1_pipeline.geometry --season 2026 --session-key 11342
python -m f1_pipeline.geometry_preview --season 2026 --session-key 11342 --self-contained --open
```

Create the Hungary replay:

```bash
python -m f1_pipeline.replay.circle_of_doom --session-key 11342 --driver VER --decision-time 2026-07-26T16:00:00Z --self-contained --output data/artifacts/circle_of_doom_hungary_2026.html
```

Add `--geometry-mode stored` to render stored geometry instead of the default synthetic circle. Resolution uses the exact session, then the same meeting, then the latest matching circuit in that season, then the legacy 2026 table, and finally the synthetic circle. An earlier `--decision-time` creates a partial replay whose last frame cannot exceed that UTC cutoff.

Build the pace analysis without fetching new data:

```bash
python -m f1_pipeline.analysis.pace --session-key 11342
```

Generated verification JSON, replay HTML, geometry previews, and analysis files are written below `data/artifacts/`. They are reproducible outputs, not source data.

Start the controlled local job service and the read-only dashboard from the repository root in separate terminals:

```bash
# Terminal 1
python -m f1_pipeline.job_service

# Terminal 2
python -m streamlit run src/f1_pipeline/dashboard/app.py
```

The CLI and local service use the same weekend orchestrator. Job status is persisted, but there is no delayed retry scheduler or automatic finalization yet.

## Pipeline and Dashboard V1 completion criteria

- A meeting selection discovers its current sessions without assuming a fixed weekend format.
- Required sessions and endpoints run through idempotent jobs with manifests and isolated status.
- Forecast snapshots and source responses remain immutable and reproducible.
- Partial publication can be rerun without replacing successful data or stopping independent jobs.
- Forecast and weather panels apply the documented point-in-time availability cut.
- The Hungary reference race can be replayed from recorded snapshots with Circle and stored-track views.
- Missing values remain missing, and source, curated, and derived data stay separated.
- The separate job service accepts deterministic selected-session, replay, and complete-weekend intents.
- The read-only UI displays curated results, standings, summaries, synchronized replay, Race Control, weather, and a clearly labelled immediate-stop projection without strategy advice.
- Unknown circuits are resolved only from uniquely validated Wikidata evidence, and replay geometry is generated from hash-verified session snapshots without blocking independent facts on failure.

## Roadmap

1. **Implemented:** Run the Hungary F1-Wikidata-Open-Meteo weekend weather pipeline with immutable evidence and an idempotent manifest.
2. **Implemented:** Ingest complete practice, qualifying, sprint-capable, and race job profiles with immutable endpoint and session manifests.
3. **Implemented:** Normalize Bronze session data into canonical Silver facts with explicit temporal availability.
4. **Implemented:** Add results, driver/team standings, wins, podium summaries, and the small read-only Dashboard V1 with a separate job-service boundary.
5. **Implemented:** Remove replay leakage from reference pace, lap progress, and stint visibility; verify one strict `decision_time` cut.
6. **Implemented:** Generalize Wikidata resolution and manifest-bound track geometry across OpenF1 seasons and circuits with fail-safe fallbacks.
7. Add trigger-driven immutable Calculation Snapshots with input hashes, versions, persisted retries, and session finalization.
7. Implement the transparent online strategy algorithm and pit-window recommendation under versioned assumptions.
8. Add qualifying and race predictions only after temporal backtests demonstrate value beyond transparent baselines.

Paid live streaming, private team-data inference, rain radar, complex optimization, and an official steward-decision feed remain outside Pipeline and Dashboard V1. The existing `pit_exit_projection` remains a hidden research artifact until a later versioned strategy service can provide recommendations.

## Documentation

- [`docs/projektdokumentation.md`](docs/projektdokumentation.md): German project report, process, verification evidence, problems, decisions, and figures
- Source cards: [OpenF1](docs/sources/openf1.md), [FastF1](docs/sources/fastf1.md), [Open-Meteo](docs/sources/open-meteo.md), and [Wikidata](docs/sources/wikidata.md)

<!--
AI assistance: OpenAI GPT-5.6 Sol and GPT-6.1 Sol 
Scope: Drafting and structuring source and interface documentation; English domain terminology.
-->
# OpenF1

**Status:** Season-parameterized discovery, Core session ingestion, replay/full extensions, results, standings, automatic local geometry, and bounded post-session scheduling are implemented.

[OpenF1](https://openf1.org/) is the primary event-oriented source. Its HTTP API provides structured historical Formula 1 data with absolute timestamps and stable session keys.

## Use in this project

| Endpoint | Main data | Use |
|---|---|---|
| `meetings`, `sessions` | keys, names, session types, start and end times | Event selection, session discovery, master data, and replay boundaries |
| `drivers` | number, acronym, team, colour | Driver and team dimensions |
| `laps`, `intervals`, `position` | lap timing, gaps, positions | Point-in-time race state |
| `pit`, `stints` | stops, compounds, tyre age | Observed strategy |
| `starting_grid` | official race grid and qualifying lap duration | Grid-to-finish comparison |
| `weather` | timestamped measurements | Session-weather observations |
| `race_control` | flags, categories, messages | Replay and track status |
| `location` | timestamped `x`, `y`, `z` | Vehicle movement and local centerline |
| `session_result`, `championship_drivers`, `championship_teams` | results, points, gaps, rank | Session results, latest standings, wins, and podium summaries |

OpenF1 is the primary source for meeting/session discovery and the replay timeline. One shared transport and Bronze path convention serve master data, weekend ingestion, replay, and verification. The pipeline discovers every advertised session, including sprint formats, before selecting sessions by purpose and `decision_time` and then selecting endpoints by session type. FastF1 overlap is retained only as a separate cross-check.

The `weekend` Core profile loads drivers, laps, stints, weather, and results where applicable. Sprint and Race also attempt championship standings. `replay` adds position, interval, pit, Race Control, and high-volume `location`; `weekend_complete_v1` applies that extension to Sprint and Race and attempts the official race grid. When one session manifest contains usable lap and location snapshots, the orchestrator hash-checks those exact immutable inputs and builds a local centerline automatically.

OpenF1 `circuit_key` is the stable join to a reviewed or uniquely auto-verified Wikidata identity. OpenF1 provides the source identity but not the WGS84 weather-reference coordinate.

## Access

- Base URL: `https://api.openf1.org/v1`
- Historical HTTP GET requests without authentication outside OpenF1's live window
- OpenF1 classifies the interval from 30 minutes before a session until 30 minutes after it as live. During an observed live window on 11 September 2026, the service also rejected an unauthenticated request for historical session `11342` and stated that global API access, including past sessions, was restricted to authenticated users until the live session ended.
- Bounded timeouts, retries with backoff, and at most 30 community requests per minute
- Immutable content-addressed Raw Parquet snapshots plus compatible latest paths under `data/raw/`

Historical jobs therefore avoid relying on anonymous access during live windows. Scheduled Core ingestion runs at `T+1 h` and retries incomplete sessions at `T+5 h`. Existing valid endpoint snapshots are reused during the retry. Authentication and live access are outside Pipeline and Dashboard V1.

## Verification

`f1_pipeline.sources.weekend_weather_pipeline` discovers and ingests Practice 1 (`11335`), Practice 2 (`11336`), Practice 3 (`11337`), Qualifying (`11338`), and Race (`11342`) instead of deriving sequential keys. Each applicable endpoint is checked independently for response shape, required fields, session identity, UTC timestamps, duplicate business keys, and Parquet read-after-write integrity.

The result records endpoint status, row count, raw and Silver paths, hashes, retrieval time, and error details in immutable session and weekend manifests. A failed endpoint does not remove successful snapshots or stop independent sessions, geometry, or weather jobs. Existing valid snapshots are reported as `stale` when reused. Geometry is persisted under `data/curated/dimensions/season=<year>/circuit_geometry.parquet` with a separate lineage manifest containing all input paths and hashes. Silver outputs cover session entries, laps, intervals, positions, pit stops, stints, Race Control events, weather observations, starting grids, session results, driver standings, team standings, and local centerlines.

## Limits

- Endpoint coverage and fields can vary by session.
- Anonymous historical access can be temporarily unavailable during an F1 live window even when the requested session is in the past.
- Free historical availability after a session is not a guaranteed publication schedule; jobs retry later without polling indefinitely.
- A complete historical table on disk is not automatically valid prediction input. Replay consumers receive only records released up to `decision_time`.
- `race_control.message` is semi-structured evidence, not an official steward-decision feed.
- OpenF1 weather is an observation source, not a substitute for an Open-Meteo forecast snapshot.
- `location.x/y/z` is approximate and has no documented geographic coordinate system.
- Location data may support relative movement and a local display centerline but cannot be placed on a world map without a validated transformation.
- Geometry resolution prefers the exact session, then its meeting, then the latest matching circuit in the selected season; 2026 additionally supports the former global table as a compatibility fallback.
- Original values, request parameters, session keys, retrieval time, and transformation metadata must remain traceable.

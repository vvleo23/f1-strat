# OpenF1

**Status:** Season-parameterized discovery, purpose-based planning, Weekend Complete V1, results, standings, and automatic local geometry are implemented; delayed scheduling and finalization remain planned.

[OpenF1](https://openf1.org/) is the primary event-oriented source. Its HTTP API provides structured historical Formula 1 data with absolute timestamps and stable session keys.

## Use in this project

| Endpoint | Main data | Use |
|---|---|---|
| `meetings`, `sessions` | keys, names, session types, start and end times | Event selection, session discovery, master data, and replay boundaries |
| `drivers` | number, acronym, team, colour | Driver and team dimensions |
| `laps`, `intervals`, `position` | lap timing, gaps, positions | Point-in-time race state |
| `pit`, `stints` | stops, compounds, tyre age | Observed strategy |
| `weather` | timestamped measurements | Session-weather observations |
| `race_control` | flags, categories, messages | Replay and track status |
| `location` | timestamped `x`, `y`, `z` | Vehicle movement and local centerline |
| `session_result`, `championship_drivers`, `championship_teams` | results, points, gaps, rank | Session results, latest standings, wins, and podium summaries |

OpenF1 is the primary source for meeting/session discovery and the replay timeline. One shared transport and Bronze path convention serve master data, weekend ingestion, replay, and verification. The pipeline discovers every advertised session, including sprint formats, before selecting sessions by purpose and `decision_time` and then selecting endpoints by session type. FastF1 overlap is retained only as a separate cross-check.

The `weekend` profile loads drivers, laps, stints, weather, and the applicable position, pit, interval, Race Control, result, and championship data. The OpenF1 `intervals` endpoint is not requested for practice and qualifying. `weekend_complete_v1` additionally persists session responses for all selected sessions and high-volume `location` for sprint and race. When one session manifest contains usable `sessions`, `laps`, and `location` snapshots, the orchestrator hash-checks those exact immutable inputs and builds a local centerline automatically.

OpenF1 `circuit_key` is the stable join to a reviewed or uniquely auto-verified Wikidata identity. OpenF1 provides the source identity but not the WGS84 weather-reference coordinate.

## Access

- Base URL: `https://api.openf1.org/v1`
- Historical HTTP GET requests without authentication for the current use case
- Bounded timeouts, retries with backoff, and at most 30 community requests per minute
- Immutable content-addressed Raw Parquet snapshots plus compatible latest paths under `data/raw/`

## Verification

`f1_pipeline.sources.weekend_weather_pipeline` discovers and ingests Practice 1 (`11335`), Practice 2 (`11336`), Practice 3 (`11337`), Qualifying (`11338`), and Race (`11342`) instead of deriving sequential keys. Each applicable endpoint is checked independently for response shape, required fields, session identity, UTC timestamps, duplicate business keys, and Parquet read-after-write integrity.

The result records endpoint status, row count, raw and Silver paths, hashes, retrieval time, and error details in immutable session and weekend manifests. A failed endpoint does not remove successful snapshots or stop independent sessions, geometry, or weather jobs. Existing valid snapshots are reported as `stale` when reused. Geometry is persisted under `data/curated/dimensions/season=<year>/circuit_geometry.parquet` with a separate lineage manifest containing all input paths and hashes. Silver outputs cover session entries, laps, intervals, positions, pit stops, stints, Race Control events, weather observations, session results, driver standings, team standings, and local centerlines.

## Limits

- Endpoint coverage and fields can vary by session.
- Free historical availability after a session is not a guaranteed publication schedule; jobs retry later without polling indefinitely.
- A complete historical table on disk is not automatically valid prediction input. Replay consumers receive only records released up to `decision_time`.
- `race_control.message` is semi-structured evidence, not an official steward-decision feed.
- OpenF1 weather is an observation source, not a substitute for an Open-Meteo forecast snapshot.
- `location.x/y/z` is approximate and has no documented geographic coordinate system.
- Location data may support relative movement and a local display centerline but cannot be placed on a world map without a validated transformation.
- Geometry resolution prefers the exact session, then its meeting, then the latest matching circuit in the selected season; 2026 additionally supports the former global table as a compatibility fallback.
- Original values, request parameters, session keys, retrieval time, and transformation metadata must remain traceable.

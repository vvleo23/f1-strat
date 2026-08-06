# OpenF1

**Status:** Implemented for all five Hungary 2026 weekend sessions; persisted scheduling and finalization remain planned.

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
| `session_result`, `championship_drivers`, `championship_teams` | results, points, rank | Results, standings, wins, and podium summaries |

OpenF1 is the primary source for meeting/session discovery and the replay timeline. The target weekend pipeline discovers every advertised session, including sprint formats, before selecting endpoints for a job profile. FastF1 overlap is retained only as a separate cross-check.

The `weekend_facts` profile loads drivers, laps, stints, weather, and the applicable position, pit, interval, and Race Control data. The OpenF1 `intervals` endpoint is not available for the verified Hungary practice and qualifying sessions and is therefore not requested for those session types. High-volume `location` remains an explicit replay or geometry input.

For the Hungary weekend weather pipeline, OpenF1 `circuit_key=4` is the stable join to the reviewed Wikidata entity `Q171356`. OpenF1 provides the identity but not the WGS84 weather-reference coordinate.

## Access

- Base URL: `https://api.openf1.org/v1`
- Historical HTTP GET requests without authentication for the current use case
- Bounded timeouts, retries with backoff, and at most 30 community requests per minute
- Immutable content-addressed Raw Parquet snapshots plus compatible latest paths under `data/raw/`

## Verification

`f1_pipeline.sources.weekend_weather_pipeline` discovers and ingests Practice 1 (`11335`), Practice 2 (`11336`), Practice 3 (`11337`), Qualifying (`11338`), and Race (`11342`) instead of deriving sequential keys. Each applicable endpoint is checked independently for response shape, required fields, session identity, UTC timestamps, duplicate business keys, and Parquet read-after-write integrity.

The result records endpoint status, row count, raw and Silver paths, hashes, retrieval time, and error details in immutable session and weekend manifests. A failed endpoint does not remove successful snapshots or stop independent sessions and weather jobs. Existing valid snapshots are reported as `stale` when reused. Silver outputs cover session entries, laps, intervals, positions, pit stops, stints, Race Control events, and weather observations.

## Limits

- Endpoint coverage and fields can vary by session.
- Free historical availability after a session is not a guaranteed publication schedule; jobs retry later without polling indefinitely.
- A complete historical table on disk is not automatically valid prediction input. Replay consumers receive only records released up to `decision_time`.
- `race_control.message` is semi-structured evidence, not an official steward-decision feed.
- OpenF1 weather is an observation source, not a substitute for an Open-Meteo forecast snapshot.
- `location.x/y/z` is approximate and has no documented geographic coordinate system.
- Location data may support relative movement and a local display centerline but cannot be placed on a world map without a validated transformation.
- Original values, request parameters, session keys, retrieval time, and transformation metadata must remain traceable.

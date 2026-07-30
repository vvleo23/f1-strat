# OpenF1

**Status:** Implemented for the Hungary 2026 race; complete weekend ingestion is planned.

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

## Access

- Base URL: `https://api.openf1.org/v1`
- Historical HTTP GET requests without authentication for the current use case
- Bounded timeouts, retries with backoff, and at most 30 community requests per minute
- Raw Parquet snapshots under `data/raw/`

## Verification

`f1_pipeline.sources.session_verification` currently discovers the Hungary meeting and race instead of guessing the session key. It does not yet ingest all weekend sessions. Each implemented endpoint is checked independently for response shape, required fields, session identity, UTC timestamps, duplicate business keys, non-empty minimum data, and Parquet read-after-write integrity.

The result records endpoint status, row count, path, retrieval time, and error details. A failed endpoint does not remove successful snapshots or stop the FastF1 check. Existing valid snapshots may be reported as `stale` when the API is unavailable.

## Limits

- Endpoint coverage and fields can vary by session.
- Free historical availability after a session is not a guaranteed publication schedule; jobs retry later without polling indefinitely.
- A complete historical table on disk is not automatically valid prediction input. Replay consumers receive only records released up to `decision_time`.
- `race_control.message` is semi-structured evidence, not an official steward-decision feed.
- OpenF1 weather is an observation source, not a substitute for an Open-Meteo forecast snapshot.
- `location.x/y/z` is approximate and has no documented geographic coordinate system.
- Location data may support relative movement and a local display centerline but cannot be placed on a world map without a validated transformation.
- Original values, request parameters, session keys, retrieval time, and transformation metadata must remain traceable.

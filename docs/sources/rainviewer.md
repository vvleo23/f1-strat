# RainViewer

**Status:** Deferred beyond the Pipeline MVP; no adapter or executable verification exists.

[RainViewer](https://www.rainviewer.com/) provides weather-radar tiles and short-term nowcast frames. It is an optional later source for spatial rain movement near a circuit, not for temperature, tyre, or trackside measurement data.

## Planned data

- radar and nowcast frame timestamps
- tile URL, zoom, coordinates, and projection metadata
- original PNG tile as Bronze evidence
- circuit coordinates and extraction radius
- decoded precipitation class or intensity with a quality flag
- processing and transformation version

## Planned access

- Retrieve metadata and only the tiles required around a circuit.
- Preserve original tiles before decoding them.
- Retain observation time, forecast valid time, request URL, attribution, and retrieval time.

## Verification before use

- Confirm frame coverage for the circuit coordinates and required time window.
- Write and read the original tile and metadata without loss.
- Validate projection, zoom, timestamps, and circuit-area extraction.
- Document how pixel classes are converted and retain the original class value.
- Treat missing frames as `partial`, `stale`, or `unavailable` without stopping independent features.

## Limits

- Tile colours are not automatically millimetres of rain.
- Radar coverage and historical retention can be incomplete.
- A nowcast frame is a forecast and must remain distinct from an observation.
- Quantitative use requires a versioned decoding method and visible uncertainty.

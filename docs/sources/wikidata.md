# Wikidata

**Status:** Planned; no adapter or executable verification exists.

[Wikidata](https://www.wikidata.org/) is the planned source for a stable geographic reference point for each circuit. This is structured source data, not a graphical map or UI feature.

## Planned data

- reviewed Wikidata entity ID
- coordinate location property `P625`
- latitude and longitude in WGS84 (`EPSG:4326`)
- circuit label, country, retrieval time, and source revision where available
- raw entity response path, content hash, and verification status

## Identity mapping

The project maintains a small reviewed mapping from the stable OpenF1 `circuit_key` to a Wikidata entity ID. Automatic fuzzy name matching is not accepted because circuit names, sponsors, cities, and translated labels can be ambiguous.

A missing or ambiguous mapping remains `unavailable`. It is never replaced with city-centre coordinates or a guessed search result.

## Planned access

- Resolve the reviewed entity through the Wikidata API or SPARQL endpoint.
- Preserve the raw response before normalizing coordinates into the `circuit` dimension.
- Refresh only when the reference mapping or source revision changes.
- Let Open-Meteo jobs read the validated circuit point instead of resolving locations themselves.

## Verification before use

- Require exactly one reviewed Wikidata entity per OpenF1 circuit key.
- Require one finite `P625` coordinate with latitude from `-90` to `90` and longitude from `-180` to `180`.
- Check circuit label, country, and locality against OpenF1 meeting metadata.
- Store source ID, retrieval time, raw path, hash, CRS, and verification result.
- Mark missing or conflicting data as `partial` or `unavailable` without blocking other circuit or session jobs.

## Limits

- Wikidata provides the weather reference point, not a detailed track, pit-lane, or racing-line geometry.
- The current replay continues to use its separate session-local OpenF1 centerline.
- Wikidata data can change and must retain source revision and retrieval metadata.
- Wikidata structured data is provided under CC0; source identity is still retained for reproducibility.

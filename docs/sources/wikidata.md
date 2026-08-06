# Wikidata

**Status:** Reviewed reference loading and candidate discovery are implemented; Hungaroring is the only approved mapping.

[Wikidata](https://www.wikidata.org/) is the source for a stable geographic reference point for each reviewed circuit. This is structured source data, not a graphical map or UI feature.

## Persisted data

- reviewed Wikidata entity ID
- coordinate location property `P625`
- latitude and longitude in WGS84 (`EPSG:4326`)
- circuit label, country, retrieval time, and source revision where available
- raw entity response path, content hash, and verification status

## Identity mapping

The pipeline loads a versioned reviewed mapping from `config/reviewed_circuit_mappings.json`. Each record links one stable OpenF1 `circuit_key` to one Wikidata entity ID and declares the expected English label and country. The implemented mapping is OpenF1 `circuit_key=4` to Hungaroring `Q171356`. The mapping schema and content hash are retained in the pipeline manifest. Automatic fuzzy name matching is not accepted because circuit names, sponsors, cities, and translated labels can be ambiguous.

A missing mapping triggers one bounded Wikidata candidate search by OpenF1 circuit name. The OpenF1 location remains separate review context. The raw search response, query, retrieval time, and hash are persisted. Candidate results remain `partial` and never provide coordinates until one identity has been reviewed and added to the registry. An empty, ambiguous, or failed search remains `unavailable`; city-centre coordinates and guessed first results are never accepted.

## Access

- Resolve the reviewed entity through the Wikidata `wbgetentities` API.
- Use `wbsearchentities` only to produce review candidates for an unmapped circuit.
- Preserve the raw response before normalizing coordinates into the `circuit` dimension.
- Refresh only when the reference mapping or source revision changes.
- Let Open-Meteo jobs read the validated circuit point instead of resolving locations themselves.

## Verification

- Require exactly one reviewed Wikidata entity per OpenF1 circuit key.
- Require one finite `P625` coordinate with latitude from `-90` to `90` and longitude from `-180` to `180`.
- Require the Wikidata label and country description to match the reviewed registry record.
- Store source ID, retrieval time, raw path, hash, CRS, and verification result.
- Mark missing or conflicting data as `partial` or `unavailable` without blocking other circuit or session jobs.

The Hungary weekend weather pipeline verified revision `2519292350`, WGS84 latitude `47.582222222222`, and longitude `19.251111111111`. Raw responses are immutable JSON snapshots under `data/raw/snapshots/wikidata/`; the enriched reference and evidence hash are stored in the Silver `circuit` dimension.

## Limits

- Wikidata provides the weather reference point, not a detailed track, pit-lane, or racing-line geometry.
- The current replay continues to use its separate session-local OpenF1 centerline.
- Wikidata data can change and must retain source revision and retrieval metadata.
- Wikidata structured data is provided under CC0; source identity is still retained for reproducibility.

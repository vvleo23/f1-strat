# Wikidata

**Status:** Reviewed reference loading and uniquely validated automatic circuit resolution are implemented.

[Wikidata](https://www.wikidata.org/) is the source for a stable geographic reference point for each reviewed circuit. This is structured source data, not a graphical map or UI feature.

## Persisted data

- reviewed or auto-verified Wikidata entity ID
- coordinate location property `P625`
- latitude and longitude in WGS84 (`EPSG:4326`)
- circuit label, country, retrieval time, and source revision where available
- raw entity response path, content hash, and verification status

## Identity mapping

The pipeline first loads the versioned manual mapping from `config/reviewed_circuit_mappings.json`. Each record links one stable OpenF1 `circuit_key` to one reviewed Wikidata identity and declares the expected English label and country. The implemented manual mapping is OpenF1 `circuit_key=4` to Hungaroring `Q171356`, and manual records always take precedence.

A missing manual mapping triggers one bounded Wikidata search by OpenF1 circuit name and loads each returned entity. Exactly one candidate must match the normalized circuit name or location, the OpenF1 country, a racing-circuit description, one non-deprecated Earth `P625`, and valid coordinate ranges. A narrow string similarity only accommodates spelling variants; it is never sufficient without all other evidence. The accepted identity is atomically merged under a bounded lock into `data/curated/registries/auto_wikidata_circuit_mappings.json`. Search and entity responses remain immutable raw evidence. Empty, invalid, or multiple valid candidates remain `partial` or `unavailable`; city-centre coordinates and guessed first results are never accepted.

## Access

- Resolve reviewed and previously auto-verified entities through `wbgetentities`.
- Use `wbsearchentities` to discover candidates for an unmapped circuit, then validate every candidate entity.
- Preserve the raw response before normalizing coordinates into the `circuit` dimension.
- Refresh only when the reference mapping or source revision changes.
- Let Open-Meteo jobs read the validated circuit point instead of resolving locations themselves.

## Verification

- Prefer one manually reviewed identity; otherwise require exactly one automatically valid candidate.
- Require one finite `P625` coordinate with latitude from `-90` to `90` and longitude from `-180` to `180`.
- Require circuit-name or location evidence, matching country text, and a racing-circuit description.
- Store source ID, retrieval time, raw path, hash, CRS, and verification result.
- Hash-check persisted auto-registry entity evidence before reuse and include both registry hashes in run identity.
- Mark missing or conflicting data as `partial` or `unavailable` without blocking other circuit or session jobs.

The Hungary reference verified revision `2519292350`, WGS84 latitude `47.582222222222`, and longitude `19.251111111111`. A live regression on 28 August 2026 uniquely auto-verified Spa-Francorchamps as `Q172851` at latitude `50.437222222222` and longitude `5.9713888888889` using a temporary registry. Raw responses are immutable JSON snapshots under `data/raw/snapshots/wikidata/`; accepted references and evidence hashes are stored in the Silver `circuit` dimension.

## Limits

- Wikidata provides the weather reference point, not a detailed track, pit-lane, or racing-line geometry.
- Replay uses a separate season-partitioned local OpenF1 centerline.
- Wikidata data can change and must retain source revision and retrieval metadata.
- Wikidata structured data is provided under CC0; source identity is still retained for reproducibility.

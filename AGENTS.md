# Project setup

- The main product is an automated Python data pipeline for a selected Formula 1 race weekend using sources such as OpenF1, FastF1, and Open-Meteo.
- Meeting selection, session discovery, ingestion jobs, validation, immutable snapshots, manifests, and scheduling take priority over models and UI.
- The binding implementation order is F1/Wikidata/Open-Meteo weekend weather pipeline, complete weekend ingestion and Silver facts, replay leakage removal, Calculation Snapshots, online strategy and pit-window recommendations, then the read-only UI.
- Historical replay is a pipeline consumer that releases data only up to `decision_time`; calculations, predictions, and dashboards must use the same temporal boundary.
- Online strategy and pit-window recommendations are MVP outputs. Rain radar is discarded and must not be added as a source, adapter, or UI feature.
- `README.md` is the Single Source of Truth for the project goal, structure, pipeline, data model, MVP, status, roadmap, setup, and usage.
- `docs/projektdokumentation.md` is the German project report for process, verification evidence, problems, decisions, research, and figures.
- `docs/sources/` contains one concise English source card per external source.
- Do not create additional documentation files unless the requested information cannot fit one of these locations.
    
# Coding rules

- Keep your code changes minimal and never refactor too much. Minimum viable Product (MVP) is the goal, not perfect code. Refactor only when necessary to implement a new feature or fix a bug.
- Do not write new comments.

- The default is **no new test**. Add one only for domain logic, DDD requirements, or public service methods that change data. Minimal tests
- Prefer a few meaningful tests for edge cases and domain scenarios over generated test volume.
- Do not test delegation without logic, read-only methods without domain rules, framework behaviour, or UI.
- Do not wire infrastructure into tests. If reflection is required, treat it as a design problem.
- No AI slop. Start with transparent domain rules or statistical baselines and add ML only after versioned features, temporal backtests, calibration, and measurable improvement exist.
- Keep ingestion, Race State, feature generation, calculations, and presentation separate.
- Persist calculation inputs, `decision_time`, trigger, input hash, feature version, calculation version, status, and output reference.

# Reliability and UI

- Isolate external-source failures. A timeout, empty response, or failed endpoint must not stop independent sources, features, or UI areas.
- Use bounded timeouts, bounded retries with backoff, and documented source rate limits. Never retry indefinitely.
- Preserve successfully loaded data when a later endpoint fails. Record raw data and error context with source, session, retrieval time, and status.
- Write data atomically and never replace the last valid snapshot with an invalid or incomplete response.
- Missing data remains missing. Never use `0`, empty defaults, or invented values to hide missing data.
- Forecast model initialization is not proof of availability. Point-in-time inputs require a documented `available_at <= decision_time`.
- Keep weather forecasts immutable and separate from later OpenF1 or FastF1 observations; observations evaluate forecasts but never rewrite them.
- Support `available`, `partial`, `stale`, and `unavailable` so processing and display can return safe partial results.
- Features with missing minimum input must return an empty state and a clear error without stopping unrelated functionality.
- Keep the UI responsive while data loads or is unavailable. All visible UI text, labels, tooltips, status messages, errors, and empty states must be English.
- Any future dashboard is a small read-only consumer of curated data and artifacts. It must not fetch sources, write snapshots, orchestrate jobs, or train models.

# Your behaviour

- Your answers should be short, precise and focused on the specific task or question at hand.
- Your code should be clean, simple, well-structured and follow the project's coding standards.
- Be critical, keep it concise, and express yourself clearly.
- You are part of the team. Say "we" instead of "I" when discussing project decisions or code changes.
- Update `README.md` whenever project scope, status, structure, data model, or usage changes.
- Keep every Markdown file English except `docs/projektdokumentation.md`.

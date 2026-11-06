# Warehouse grains and definitions

| Table | Grain / key | Purpose |
|---|---|---|
| flights | Current accepted business key, hashed as flight_id | One reporting-carrier scheduled flight; all statuses retained |
| partition_versions | version_id | Immutable source/reference/transform version, first load and counts |
| checkpoints | partition_month | Currently active source version |
| partition_activations | run_id + month + activation time | Full month replacements and their changes |
| flight_changes | activation run + version + flight_id | Revised or withdrawn records, old/new hashes and selected values |
| rotation_candidates | One assigned current flight | Previous reported leg and link eligibility reason |
| fact_turnarounds | outbound_flight_id | One eligible inbound/outbound pair |
| dim_airport | IATA airport code | Observed BTS IDs, city, timezone, coordinates |
| dim_carrier | DOT reporting carrier ID | Observed code and flight count |
| dim_date | flight_date | Calendar fields |
| mart_daily_reliability | flight_date | Counts, cancellation rate, conditional arrival reliability |
| mart_airport_daily | flight_date + origin airport | Departure counts and measures |
| mart_routes | origin + destination | Route aggregates for the whole loaded cohort |
| mart_turnaround_airports | Turn airport | Turn counts, conditional lateness, median recovery |
| mart_turn_bands | Planned-turn duration band | Conditional lateness by observational cohort |
| mart_reported_causes | Delay-cause category | Sum of reported minutes and non-null record count |
| mart_link_coverage | Link status | Coverage/exclusion audit |
| pipeline_runs | run_id | Start, end, status and error |
| task_events | One task event | Ingestion/model/publication audit |
| quality_checks | Run + month + rule | Contract and time-validation results |
| publications | publication_id | Report path, metrics and build configuration |

## Selected flight columns

`flight_date` is the BTS local flight service date, not a UTC calendar date. HHMM source fields are preserved. UTC timestamp columns are timezone-aware in DuckDB and Parquet. Airport timezones come from the pinned current reference; unresolved mappings are never assigned a guessed timezone.

`carrier_id` is the DOT reporting carrier ID. `carrier` is the reported code. `tail_number` is nullable, normalized to uppercase, and may not represent a globally complete or permanent aircraft identity. `flight_number` is the reporting-carrier number.

`dep_delay_min` and `arr_delay_min` are signed: early is negative. Missing values are null, not zero. `cancelled` and `diverted` are 0/1. The original cancellation category is retained in `cancellation_code`.

`schedule_clock_ok`, `actual_clock_ok`, `time_valid`, and `time_status` expose the time reconstruction's confidence gates. `source_row` points to the source CSV data row (1-based, after the header). `version_id` links to the raw ZIP, selected CSV provenance and complete source metadata. `record_hash` hashes the selected source business fields; derived timestamps also depend on the captured reference and transform version.

## Turnaround measures

All durations/delays are minutes. Distances in the flight warehouse are statute miles, matching BTS. Coordinates are decimal longitude/latitude. `inbound_late` and `outbound_late` mean >=15 minutes. `recovered_delay_min` can be negative. `added_delay_min` is its negative. Neither implies attribution to a particular cause.

The first observed assigned flight receives `no_previous_observation`. `airport_discontinuity` can indicate a missing movement; it is not silently connected. `long_gap` exceeds the configured 12-hour default on either clock. `unorderable_tail_day`, `invalid_clock`, `schedule_overlap`, `actual_overlap`, `cancelled_leg`, `diverted_leg`, and `carrier_changed` exclude links. Records without tail numbers cannot enter the candidate table; their count is still available in flights.

## Historical versions

Raw files are `bronze/<month>/<sha256>.zip`; original source metadata is alongside each ZIP. `bronze/<month>/latest.json` identifies the most recently acquired file, which may be newer than the committed database checkpoint after an interrupted load. The **database checkpoint** is the authority for the active warehouse version.

Every versioned silver Parquet contains its own complete accepted monthly snapshot. Read an old path from `partition_versions.silver_relative_path` to inspect that version. The active `flights` table contains only current monthly snapshots. Acquisition timestamps describe this system's capture time, not BTS's initial publication date or the time the flight operated.

# Data dictionary

## Conventions

Flight date is the BTS scheduled departure date in the origin's local calendar. Month is YYYY-MM from that date, not UTC departure month. Connections use the outbound flight's month. Reconstructed/audit instants are PostgreSQL timestamptz in UTC and Parquet UTC timestamps; source HHMM integers remain local wall clocks. 2400 means next midnight. Delays are signed minutes; distance is statute miles. Missing values remain null.

`flight_id` is SHA-256 of canonical date|DOT carrier ID|flight number|origin ID|destination ID|scheduled departure HHMM. Tail is excluded so tail corrections retain flight identity. A natural-key correction appears as withdrawal+insertion. `record_hash` fingerprints normalized source content. `version_id` hashes source ZIP, pinned reference snapshot, transform version, implementation/package versions, month and mode. Reference-only changes can change the version without changing source record hashes.

## Tables, grains and history

| Relation | Grain/key | Relationships and behavior |
|---|---|---|
| raw.flights | One current accepted flight / flight_id | Version FK; complete-month transactional replacement; cancellations/diversions retained |
| raw.sequence_barriers | Month + tail + local date | Rejected movement blocks links around that tail/day |
| raw.blocked_months | Month | Unassignable reject conservatively blocks month connections |
| analytics.stg_flights | One current raw flight | Thin source view |
| analytics.fact_flights | Flight / flight_id | Incremental changed-month replacement; origin/destination → dim_airports, carrier → dim_carriers, date → dim_dates |
| analytics.int_aircraft_observations | Assigned-tail accepted flight | Includes cancellation/diversion/invalid clocks; adds unorderable-tail-day and quarantine flags |
| analytics.int_aircraft_connections | Outbound observation / flight_id | Nullable previous_flight_id → facts; lag/cumulative max windows over all loaded months |
| analytics.fact_turnarounds | Eligible inbound→outbound pair / outbound_flight_id | Both flight IDs → facts; airport ID → dimension; whole-cohort rebuild |
| analytics.fact_disruptions | Accepted flight / flight_id | Flags, reported delay values and classified arrival outcome; includes non-disrupted denominator |
| analytics.dim_airports | BTS airport ID | Latest observed code/city; pinned current reference coordinates/timezone, not SCD2 |
| analytics.dim_carriers | DOT reporting-carrier ID | Latest observed code; no invented corporate history |
| analytics.dim_dates | Observed local date | Year/month/ISO weekday/week; absent dates are not zero-traffic rows |
| mart_daily_reliability | Local date | Counts, delay mean/p90, on-time and cancellation rates |
| mart_airport_daily | Date + origin code | Legacy departure-focused aggregation |
| mart_routes | Origin + destination code | Route reliability aggregation |
| mart_turnaround_airports | Connection airport code | Eligible counts, planned/actual medians, conditional delay metrics |
| mart_turn_bands | Planned gap band | <45, 45–59, 60–89, 90–119, 120+ minutes |
| mart_reported_causes | Five BTS cause categories | Reported minutes and nonnull-record count |
| mart_link_coverage | Link status | Assigned-tail candidate counts including first observation |
| mart_monthly_airport_performance | Source month + BTS airport ID | Departure/arrival/turn denominators; full join preserves arrivals-only airports |
| mart_pipeline_audit | Committed month | Checkpoint/version hashes, acquisition time, coverage and reconciliation |

All marts are in analytics. dbt tests enforce logical keys/relationships; analytics foreign keys are not claimed as physically installed. Raw/meta have physical primary/foreign/check constraints. Dimensions and connection/mart models rebuild; flight facts alone use month-aware increments. Raw/source versions and gold snapshots retain historical states; current facts expose the active version.

## Flight field meanings

The exact BTS→warehouse mapping and types are appended below from the implementation.

- Carrier/DOT IDs identify reporting carriers; airport IDs are BTS IDs, codes are source IATA codes. Cities are source labels.
- Tail number may be absent: such flights remain in flight metrics but not aircraft sequencing.
- Scheduled/actual HHMM values are original local clocks. Scheduled elapsed and actual elapsed are minutes.
- Scheduled departure UTC localizes origin date+clock. Scheduled arrival adds scheduled elapsed. Actual departure adds signed departure delay; actual arrival adds actual elapsed. Destination clocks and arrival delay are checked with one-minute tolerance.
- Ambiguous/nonexistent DST times remain unresolved. `time_valid` guards reliable plots/links; invalid derived values may remain for audit. `time_status` records the reason.
- Cancellation/diversion are binary flags. Cancellation code is nullable. Taxi/distance/cause quantities are nonnegative where provided; absent cause/delay values are never imputed as zero.
- Coordinate degrees come from the pinned reference, may be null for unknown codes, and must lie within ±90 latitude/±180 longitude.
- `source_row` is the one-based data-row index excluding the CSV header. Flight/content/version hashes establish lineage.

## Connection fields

`previous_flight_id` is the preceding reported observation, not a confirmed physical movement. `latest_prior_arrival` is the cumulative maximum actual arrival among all prior ordered observations, protecting against overlap beyond the immediate predecessor. Ordering uses scheduled UTC departure then flight ID; unresolved schedules conservatively flag a tail/date.

Statuses, in SQL priority order: no_previous_observation, carrier_changed, quarantined_movement, unorderable_tail_day, cancelled_leg, diverted_leg, invalid_clock, airport_discontinuity, schedule_overlap, actual_overlap, long_gap, eligible. The first matching reason is reported; multiple problems may exist.

Turnaround facts include inbound/outbound IDs, tail/carrier, airport/date/month, inbound UTC arrival, outbound UTC departure, planned/actual gap, inbound arrival delay, outbound departure delay, recovered/added delay, lateness flags and planned-gap band. See METRICS.md.

## Audit records and files

| Relation | Grain/key and purpose |
|---|---|
| meta.project | Singleton database mode guard |
| meta.pipeline_runs | Run ID; start/end/status/months/error |
| meta.task_events | Identity event ID; started/succeeded/failed attempts retained |
| meta.partition_versions | Version ID; immutable source/reference hashes, counts, first load and manifest |
| meta.checkpoints | Month → committed version; authoritative watermark |
| meta.partition_activations | Identity activation ID; previous/current version and inserted/updated/withdrawn counts; reactivation creates a new event |
| meta.flight_changes | Activation + flight; old/new values for updates and withdrawals; new insertions are counted and recoverable in snapshots |
| meta.quality_results | Identity quality event; reconciliation and dbt summary |
| meta.model_build | Singleton completed model source signature and max interval |
| meta.publications | Publication ID; source/model identity and file checksums |

Not every audit/run relationship is a physical FK because recovery events can be written independently.

Raw `YYYY-MM/<sha>.zip` and first-capture JSON are immutable. Raw latest.json means latest acquired, not necessarily committed. Silver `month=.../version=.../flights.parquet` has a manifest/checksum. Quarantine stores invalid/conflicting rows and quality summaries. Job JSON journals completed stages/errors; adjacent files store dbt logs/results. Gold `<publication-id>/` contains tested Parquet and its manifest; latest.json promotes the last successful publication. Runtime paths are excluded from Git.

## Exact flight schema

| Column | PostgreSQL type | BTS source / derivation |
|---|---|---|
| flight_date | date | FlightDate |
| carrier | text | Reporting_Airline |
| carrier_id | bigint | DOT_ID_Reporting_Airline |
| tail_number | text | Tail_Number |
| flight_number | bigint | Flight_Number_Reporting_Airline |
| origin_id | bigint | OriginAirportID |
| origin | text | Origin |
| origin_city | text | OriginCityName |
| dest_id | bigint | DestAirportID |
| dest | text | Dest |
| dest_city | text | DestCityName |
| scheduled_dep_hhmm | bigint | CRSDepTime |
| scheduled_arr_hhmm | bigint | CRSArrTime |
| actual_dep_hhmm | bigint | DepTime |
| actual_arr_hhmm | bigint | ArrTime |
| dep_delay_min | double precision | DepDelay |
| arr_delay_min | double precision | ArrDelay |
| scheduled_elapsed_min | double precision | CRSElapsedTime |
| actual_elapsed_min | double precision | ActualElapsedTime |
| cancelled | bigint | Cancelled |
| cancellation_code | text | CancellationCode |
| diverted | bigint | Diverted |
| taxi_out_min | double precision | TaxiOut |
| taxi_in_min | double precision | TaxiIn |
| distance_miles | double precision | Distance |
| carrier_delay_min | double precision | CarrierDelay |
| weather_delay_min | double precision | WeatherDelay |
| nas_delay_min | double precision | NASDelay |
| security_delay_min | double precision | SecurityDelay |
| late_aircraft_delay_min | double precision | LateAircraftDelay |
| source_row | bigint | Derived; see conventions above |
| origin_tz | text | Derived; see conventions above |
| origin_lat | double precision | Derived; see conventions above |
| origin_lon | double precision | Derived; see conventions above |
| dest_tz | text | Derived; see conventions above |
| dest_lat | double precision | Derived; see conventions above |
| dest_lon | double precision | Derived; see conventions above |
| scheduled_dep_utc | timestamptz | Derived; see conventions above |
| scheduled_arr_utc | timestamptz | Derived; see conventions above |
| actual_dep_utc | timestamptz | Derived; see conventions above |
| actual_arr_utc | timestamptz | Derived; see conventions above |
| schedule_clock_ok | boolean | Derived; see conventions above |
| actual_clock_ok | boolean | Derived; see conventions above |
| time_valid | boolean | Derived; see conventions above |
| time_status | text | Derived; see conventions above |
| month | text | Derived; see conventions above |
| flight_id | text | Derived; see conventions above |
| record_hash | text | Derived; see conventions above |
| version_id | text | Processing version foreign key |

# Sources, attribution and interpretation

## Flight records

U.S. Department of Transportation, Bureau of Transportation Statistics, Reporting Carrier On-Time Performance (1987–present).

- Monthly archives: https://transtats.bts.gov/PREZIP/
- Database profile: https://www.transtats.bts.gov/DatabaseInfo.asp?QO_VQ=EFD
- Definitions and reporting instructions: https://www.bts.gov/explore-topics-and-geography/modes/aviation/number-40-technical-directive-reporting-time

Default archives are January, February and March 2025. The project retrieves public files without authentication. The source covers reporting domestic operations, not every airline or every movement. Use the original BTS definitions when interpreting delays and cancelled/diverted flights. A source snapshot can be revised later.

The three-day demo includes all rows whose source FlightDate is March 8, 9 or 10, 2025. The CSV was reserialized and its empty trailing unnamed column removed; it is not the original monthly ZIP. `demo/source.json` identifies the selection, original ZIP checksum, new checksum, original URL, and capture time. No flight values are invented or replaced. Unit tests use separate, clearly synthetic fixtures.

## Airport/timezone reference

airportsdata, version 20250909, by Matteo Borsetti and contributors: https://github.com/mborsetti/airportsdata

This project uses IATA codes, coordinates, cities and IANA timezone names. airportsdata is released under the MIT license; its license text is included in `assets/AIRPORTSDATA_LICENSE.txt`. Its data is current rather than historical, and the publisher does not guarantee accuracy. Missing airports are surfaced rather than guessed. The project pins tzdata 2025.2 and resets Python's timezone path to use that package consistently.

## Map geometry

Natural Earth, 1:110m country outlines, public domain: https://www.naturalearthdata.com/about/terms-of-use/

Geometry source: https://github.com/nvkelso/natural-earth-vector/blob/master/geojson/ne_110m_admin_0_countries.geojson

The included file retains U.S., Canadian and Mexican features for map context. Route segments connect endpoints and are not observed aircraft trajectories.

## Analytical limits

- Results describe the loaded reporting cohort. They are not global airline coverage.
- A valid pair is conservative evidence of adjacent reported legs, not proof that every intervening movement is known.
- Conditional delay rates and differences between turnaround bands are observational, not causal effects or safe operational targets.
- Source-reported delay causes are not inferred from the pair analysis.
- There is no flight prediction model, impact simulation, claimed cost saving or production SLA.
- The public HTML report is fully self-contained; it does not contact external APIs when viewed.
- The notebook includes ordinary execution outputs when you run it, and the pipeline writes operational logs. No conversation transcript or assistant tool history is embedded.

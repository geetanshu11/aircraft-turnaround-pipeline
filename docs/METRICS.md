# Metrics and sources

Primary source: [BTS Reporting Carrier On-Time Performance](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ). Airport coordinates/IANA zones use pinned airportsdata; timezone rules use pinned tzdata. All results are descriptive, not causal.

| Metric | Formula and denominator | Model |
|---|---|---|
| Reported flights | Accepted source records, including cancellations/diversions | fact_flights |
| Cancellation rate | 100 × cancelled / accepted scheduled records | Daily/monthly marts |
| Measured arrival | Noncancelled, nondiverted and nonnull reported arrival delay | Daily/monthly marts |
| On-time arrival % | 100 × measured arrivals with delay <15 / measured arrivals | Dashboard sums counts before division; never averages daily percentages |
| Late departure | Noncancelled departure delay ≥15; measured denominator requires nonnull delay | Airport-month model |
| Mean/p90 arrival delay | Mean/continuous 90th percentile of signed reported delay among noncancelled/nondiverted flights | Daily reliability; null excluded, early arrivals negative |
| Planned interval | Outbound scheduled departure − preceding inbound scheduled arrival, minutes | Eligible pairs only |
| Actual interval | Outbound actual departure − preceding inbound actual arrival, minutes | Eligible pairs only; not ground-service time |
| Eligible pair | Same nonmissing tail/carrier, matching airport code AND BTS ID, valid clocks, no cancellation/diversion/barrier/overlap, both gaps 0 through configured maximum inclusive | int_aircraft_connections; default max 720 minutes |
| Recovered delay | Inbound arrival delay − outbound departure delay | fact_turnarounds; may be negative |
| Added delay | Negative of recovered delay | fact_turnarounds |
| Conditional late-outbound % | 100 × eligible pairs with inbound delay ≥15 and outbound delay ≥15 / eligible pairs with inbound delay ≥15 | Airport, turn-band and monthly marts |
| Recovered to on time | Inbound delay ≥15 and outbound delay <15 among eligible pairs | Turnaround-airport mart |
| Median recovery | Continuous median recovered delay among late-inbound eligible pairs | Bands/airport marts |
| Reported cause minutes | Sum of each nonnull BTS cause field, plus nonnull-record count | mart_reported_causes; missing ≠ zero |
| Connection coverage | Candidate counts by link status among flights with tail IDs | Includes first observations; missing-tail flights are outside this cohort |
| Monthly airport performance | Departures by origin ID, arrivals by destination ID, turns by connection airport; source/outbound month | Independent denominators retained |
| Coverage days | Distinct accepted local flight dates | Manifest/audit and airport departure-day count; not proof of national completeness |
| Freshness | Latest flight date, source acquisition, commit and publication timestamps separately | Audit/publication/journals |
| Pipeline health | Last job status/error/lease alongside last successful publication | Current failure may coexist with an older valid dashboard |

Zero denominators yield undefined/null percentages. Source delay metrics do not require reconstructed UTC clocks. Coverage must be comparable: March in the demo contains only three days.

Dashboard 95% Wilson intervals for conditional proportions use z=1.96, center `(p+z²/(2n))/(1+z²/n)` and half-width `z*sqrt(p(1-p)/n+z²/(4n²))/(1+z²/n)`. They are undefined for n=0. Repeated aircraft/shared disruptions violate independent sampling; treat these as descriptive uncertainty bands.

Unreported ferry/maintenance movements, excluded carriers, missing registrations and reassigned tail numbers limit inferred physical continuity. Passing SQL rules does not establish an observed ground-service duration or a causal effect of a turnaround policy.

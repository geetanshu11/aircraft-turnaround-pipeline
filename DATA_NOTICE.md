# Data and source attribution

Flights: U.S. Department of Transportation Bureau of Transportation Statistics, Reporting Carrier On-Time Performance. Source: https://www.transtats.bts.gov/PREZIP/ . Fields: https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ . Educational portfolio use does not imply endorsement.

The unchanged real demo contains 58,357 flights from March 8–10, 2025. SHA-256: 6153012b1ad63cbfa1a3fe73cdc061a86e998ed681da881f1fdced94767f9ce6. demo/source.json preserves original provenance. Full raw months are downloaded at runtime and excluded from Git. tests/helpers.py creates deterministic synthetic records explicitly for tests.

Airport coordinates/IANA zones: airportsdata 20250909; its license is preserved at notebooks/legacy/source/assets/AIRPORTSDATA_LICENSE.txt. Timezone rules: tzdata 2025.2. References are snapshotted/hashed but not historically complete registries.

The original notebook and complete embedded project remain in notebooks/legacy/. No ownership or new license grant over the original author's work is invented. Third-party licenses remain with their distributions.

Reported tail observations may omit physical movements. Inferred intervals are not directly observed ground-service durations; conditional delay associations are not causal effects.

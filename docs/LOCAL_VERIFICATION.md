# Local verification — November 2026

Checked on Windows 11 with Docker Desktop and WSL 2 (16 GB system RAM; 6 GB WSL limit).

| Check | Observed result |
|---|---|
| Full dataset | **1,645,503 flights**, January–March 2025, shown on the Live dashboard and approximately confirmed by a direct PostgreSQL count |
| Full dataset dbt checks | **53 passed**, 0 failures or warnings |
| Full dataset publication | Live dashboard reported a succeeded run and published data through March 31, 2025 |
| Three-day demo | **58,357 flights** confirmed by direct PostgreSQL SQL count |
| Demo Airflow run | **9/9 tasks succeeded**, also confirmed by the corrected local validation script |
| Python tests | **55 passed, 6 skipped** in the non-PostgreSQL run |
| Ruff | Lint and format checks passed |

**Still open:** GitHub Actions remote CI was queued and has not been verified as passing; six skipped Python tests remain to be assessed separately. We also haven't verified cloud deployment, unattended scheduling, or a full-quarter Airflow-orchestrated run.

An earlier diagnostic incorrectly finished without task instances, and a subsequent `airflow dags test` diagnostic hit a task-state conflict. Those issues were addressed for the demo validation; the successful nine-task scheduler-run result is the evidence used here.

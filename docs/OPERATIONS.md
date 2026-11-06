# Running and troubleshooting

The commands below assume PowerShell is open in the project folder and Docker Desktop is running.

## Basic checks

```powershell
docker compose ps -a
docker compose --profile tools run --rm pipeline health --mode demo
```

Run the bundled demo with `.\scripts\run-demo.ps1`; initialize with `.\scripts\setup.ps1 -Profile lite`. To use Airflow, run `.\scripts\setup.ps1 -Profile full` and visit <http://localhost:8080>. Use `.\scripts\validate-local.ps1 -Full` only when the Airflow services are healthy and the DAG is unpaused.

## Stop and restart

```powershell
docker compose --profile full stop
docker compose up -d --wait
```

`stop` preserves Docker volumes, including the PostgreSQL databases and published snapshots. `down -v` **deletes project volumes**; do not use it for normal shutdown or troubleshooting. Full mode can be restarted through `.\scripts\setup.ps1 -Profile full`, but that also rebuilds and reinitializes services.

## When something fails

```powershell
docker compose --profile full ps -a
docker compose --profile full logs --tail 100 postgres dashboard airflow-api airflow-scheduler airflow-dag-processor
```

If a download fails, examine the error before retrying. If a data check fails, fix the source/schema issue rather than removing the test. If a run stopped halfway through, the pipeline records its job state. Inspect the active job before using `resume` or `abandon`, and never launch two writers against the same demo/live dataset. Airflow can retry failed tasks independently.

An existing job path can be printed with:

```powershell
docker compose --profile tools run --rm --entrypoint python pipeline -c "from pathlib import Path; print(Path('/data/demo/active_job.json').read_text())"
```

If it exists and no process is still running, you can use `docker compose --profile tools run --rm pipeline resume --job <job-path>` with the *actual* job path from that file. Do not paste `<job-path>` literally.

## Data and security

- Demo and full data use separate `aircraft_demo` and `aircraft_live` databases.
- `.env` contains generated local passwords. Keep it out of Git and screenshots.
- Before any major change, back up the database and data volumes, or work in a separate checkout. A Git commit backs up code, **not** Docker volumes.
- SQL changes may require rerunning transformations and checks. Full-month historical corrections need explicit refresh/reprocessing.
- For field meanings and limitations, see [data dictionary](DATA_DICTIONARY.md) and [metrics](METRICS.md).

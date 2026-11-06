param([switch]$Full)
. "$PSScriptRoot/common.ps1"
Assert-Docker
Assert-Environment
& "$PSScriptRoot/run-demo.ps1"
Invoke-Docker compose exec -T postgres psql -U platform_admin -d aircraft_demo -v ON_ERROR_STOP=1 -c 'SELECT count(*) AS flights FROM analytics.fact_flights; SELECT count(*) AS inferred_turns FROM analytics.fact_turnarounds; SELECT * FROM analytics.mart_pipeline_audit;'
if ($Full) {
    Invoke-Docker compose --profile full exec -T airflow-scheduler python /opt/platform/scripts/check_dag.py
    # Use a real scheduler-controlled run. `airflow dags test` can mask task-state
    # conflicts in Airflow 3, so the helper asserts nine persisted successes.
    Invoke-Docker compose --profile full exec -T airflow-api python /opt/platform/scripts/test_dag_run.py
}
Write-Host 'PASS: local validation steps finished successfully.'

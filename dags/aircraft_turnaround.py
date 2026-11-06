"""Actual Airflow TaskFlow DAG; no network or database work during parsing."""

from datetime import timedelta
import os
import subprocess
import pendulum
from airflow.sdk import dag, task, get_current_context, Param

STAGES = (
    "download",
    "validate",
    "raw_storage",
    "parquet_processing",
    "warehouse_loading",
    "dbt_transformation",
    "quality_tests",
    "analytical_publishing",
)


def invoke(arguments):
    result = subprocess.run(
        [os.getenv("PLATFORM_PYTHON", "/opt/platform-venv/bin/python"), "-m", "turnaround.cli", *arguments],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    print(result.stdout, flush=True)
    result.check_returncode()
    return result.stdout.strip().splitlines()[-1]


@dag(
    dag_id="aircraft_turnaround",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=os.getenv("AIRCRAFT_SCHEDULE") or None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=6),
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=2),
    },
    params={
        "mode": Param("demo", enum=["demo", "live"]),
        "months": Param([], type="array", items={"type": "string", "pattern": r"^20\d{2}-(0[1-9]|1[0-2])$"}),
        "refresh": Param(False, type="boolean"),
    },
    tags=["bts", "portfolio", "local"],
)
def aircraft_turnaround():
    @task(task_id="prepare_run")
    def prepare():
        context = get_current_context()
        params = context["params"]
        scheduled = str(context["dag_run"].run_type) in {"scheduled", "DagRunType.SCHEDULED"}
        mode = "live" if scheduled else params["mode"]
        months = params["months"]
        if scheduled:
            anchor = context.get("data_interval_end") or context["logical_date"]
            months = [anchor.subtract(months=k).format("YYYY-MM") for k in [5, 4, 3]]
        if not months:
            months = ["2025-03"] if mode == "demo" else ["2025-01", "2025-02", "2025-03"]
        args = ["plan", "--mode", mode, "--months", *months, "--run-id", context["run_id"]]
        if scheduled or params["refresh"]:
            args.append("--refresh")
        return invoke(args)

    @task
    def execute(job_path: str, stage_name: str):
        invoke(["stage", stage_name, "--job", job_path])
        return job_path

    previous = prepare()
    for name in STAGES:
        previous = execute.override(task_id=name)(previous, name)


aircraft_turnaround()

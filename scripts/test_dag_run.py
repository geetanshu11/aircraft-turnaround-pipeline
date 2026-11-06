"""Verify a real scheduled-by-Airflow execution, not an in-process `dags test`.

Triggers a unique demo run and checks the metadata DB (read-only SELECTs)
until all nine expected task instances have persisted successful states.
The trigger itself runs the normal idempotent demo pipeline.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import create_engine, text

DAG_ID = "aircraft_turnaround"
EXPECTED = frozenset(
    (
        "prepare_run",
        "download",
        "validate",
        "raw_storage",
        "parquet_processing",
        "warehouse_loading",
        "dbt_transformation",
        "quality_tests",
        "analytical_publishing",
    )
)
MAX_WAIT_SECONDS = 600
POLL_SECONDS = 3


def validate_terminal_state(state: str | None, task_states: dict[str, str | None]) -> bool:
    """Return True only for a successful DAG with nine successful task instances.

    Raises on any terminal result that is not a complete success.
    """
    if state not in ("success", "failed"):
        return False
    if state == "failed":
        raise RuntimeError(f"DAG run failed. Task states: {task_states}")
    missing = EXPECTED - task_states.keys()
    extra = task_states.keys() - EXPECTED
    incomplete = {name: value for name, value in task_states.items() if value != "success"}
    if missing or extra or incomplete:
        raise RuntimeError(
            "DAG was marked successful without nine successful task instances. "
            f"Missing: {sorted(missing)}. Extra: {sorted(extra)}. "
            f"Non-success: {incomplete}"
        )
    return True


def main() -> None:
    db_url = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    if not db_url:
        raise RuntimeError("Airflow metadata database connection is not configured")
    engine = create_engine(db_url, pool_pre_ping=True)

    # Fail safely if Airflow cannot run tasks: no pointless queued run.
    with engine.connect() as conn:
        paused = conn.execute(
            text("SELECT is_paused FROM dag WHERE dag_id = :dag_id"),
            {"dag_id": DAG_ID},
        ).scalar_one_or_none()
    if paused is None:
        raise RuntimeError(f"DAG {DAG_ID!r} is not registered; check DAG processor logs")
    if paused:
        raise RuntimeError(f"DAG {DAG_ID!r} is paused; turn it ON in Airflow first")

    now = datetime.now(timezone.utc)
    logical_date = now.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    run_id = f"validation__{now:%Y%m%dT%H%M%S}Z__{uuid4().hex[:10]}"
    config = {"mode": "demo", "months": ["2025-03"], "refresh": False}

    print(f"Triggering Airflow run {run_id}", flush=True)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "airflow",
            "dags",
            "trigger",
            "--run-id",
            run_id,
            "--logical-date",
            logical_date,
            "--conf",
            json.dumps(config, separators=(",", ":")),
            DAG_ID,
        ],
        check=True,
    )

    deadline = time.monotonic() + MAX_WAIT_SECONDS
    previous_summary = None
    while time.monotonic() < deadline:
        with engine.connect() as conn:
            state = conn.execute(
                text("SELECT state FROM dag_run WHERE dag_id = :dag_id AND run_id = :run_id"),
                {"dag_id": DAG_ID, "run_id": run_id},
            ).scalar_one_or_none()
            rows = conn.execute(
                text("SELECT task_id, state FROM task_instance WHERE dag_id = :dag_id AND run_id = :run_id"),
                {"dag_id": DAG_ID, "run_id": run_id},
            ).all()

        task_states = {row[0]: row[1] for row in rows}
        summary = (state, len(rows), sum(x == "success" for x in task_states.values()))
        if summary != previous_summary:
            print(
                f"Run status={state or 'pending'}, "
                f"tasks={len(rows)}/{len(EXPECTED)}, "
                f"successful={summary[2]}/{len(EXPECTED)}",
                flush=True,
            )
            previous_summary = summary
        if validate_terminal_state(state, task_states):
            print("PASS: Airflow run completed with all nine tasks successful", flush=True)
            return
        time.sleep(POLL_SECONDS)

    raise TimeoutError(
        f"Airflow run {run_id} did not finish within {MAX_WAIT_SECONDS} seconds. "
        "Inspect its state and task logs in http://localhost:8080."
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"FAIL: Airflow end-to-end validation: {exc}", file=sys.stderr)
        sys.exit(1)

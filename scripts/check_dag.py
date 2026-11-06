from pathlib import Path
import os
from airflow.dag_processing.dagbag import DagBag

root = Path(__file__).resolve().parents[1]
bag = DagBag(dag_folder=os.getenv("AIRFLOW__CORE__DAGS_FOLDER", str(root / "dags")))
assert not bag.import_errors, bag.import_errors
assert set(bag.dags) == {"aircraft_turnaround"}
dag = bag.dags["aircraft_turnaround"]
expected = [
    "prepare_run",
    "download",
    "validate",
    "raw_storage",
    "parquet_processing",
    "warehouse_loading",
    "dbt_transformation",
    "quality_tests",
    "analytical_publishing",
]
assert {t.task_id for t in dag.tasks} == set(expected)
for a, b in zip(expected, expected[1:]):
    assert dag.get_task(a).downstream_task_ids == {b}
assert dag.max_active_runs == 1 and all(t.retries == 2 for t in dag.tasks)
assert [t.task_id for t in dag.topological_sort()] == expected
print("PASS: real Airflow import, nine tasks, dependencies, retries and acyclic graph")

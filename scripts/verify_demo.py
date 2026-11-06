"""Real-data E2E and idempotency acceptance check."""

import json
from pathlib import Path
from turnaround.core import Config
from turnaround.pipeline import run_pipeline
from turnaround.warehouse import connect

cfg = Config.from_env(mode="demo")
a = run_pipeline(cfg, ["2025-03"])
b = run_pipeline(cfg, ["2025-03"])
assert (
    a["publication"]["publication_id"] == b["publication"]["publication_id"] and b["loaded"][0]["status"] == "unchanged"
)
with connect(cfg) as con:
    flights = con.execute("SELECT count(*) n FROM analytics.fact_flights").fetchone()["n"]
    turns = con.execute("SELECT count(*) n FROM analytics.fact_turnarounds").fetchone()["n"]
    server = con.execute("SELECT version() v").fetchone()["v"]
assert flights == 58357 and turns == 49662, (flights, turns)
result = {
    "flights": flights,
    "eligible_turns": turns,
    "dbt_tests": a["quality_test_count"],
    "publication_id": a["publication"]["publication_id"],
    "rerun_load_status": "unchanged",
    "server_version": server,
}
Path("artifacts").mkdir(exist_ok=True)
Path("artifacts/real-demo.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))

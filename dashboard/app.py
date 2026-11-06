"""Read-only tested Parquet analytics; no warehouse credentials."""

import json
import os
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Aircraft Turnaround Intelligence", page_icon="✈", layout="wide")
st.title("Aircraft Turnaround & Disruption Intelligence")
st.caption("BTS flights • Inferred aircraft connections • Operational and pipeline analytics")
mode = st.sidebar.selectbox("Dataset", ["demo", "live"])
root = Path(os.getenv("DATA_ROOT", "runtime")) / mode
st.sidebar.info("Demo covers March 8–10, 2025 only. Live contains your downloaded full months.")
if not (root / "gold/latest.json").exists():
    st.info("No tested publication is available yet.")
    st.code("docker compose --profile tools run --rm pipeline run --mode " + mode + " --months 2025-03")
    st.stop()
p = json.loads((root / "gold/latest.json").read_text())
folder = (root / p["relative_path"]).resolve()
if not folder.is_relative_to(root.resolve()):
    raise ValueError("Invalid publication path")
manifest = json.loads((folder / "publication.json").read_text())


@st.cache_data(show_spinner=False)
def read_table(path):
    return pd.read_parquet(path)


def table(name):
    return read_table(str(folder / (name + ".parquet")))


@st.cache_data(show_spinner=False)
def query(path, sql, values=()):
    with duckdb.connect() as con:
        return con.execute(sql, [path, *values]).to_arrow_table().to_pandas()


daily = table("mart_daily_reliability")
audit = table("mart_pipeline_audit")
measured = daily.measured_arrivals.sum()
ontime = 100 * (measured - daily.late_arrivals.sum()) / measured if measured else np.nan
m = st.columns(4)
m[0].metric("Reported flights", f"{int(daily.scheduled_flights.sum()):,}")
m[1].metric("Eligible inferred intervals", f"{manifest['files']['fact_turnarounds.parquet']['rows']:,}")
m[2].metric("On-time measured arrivals", f"{ontime:.1f}%")
m[3].metric("Cancelled flights", f"{int(daily.cancellations.sum()):,}")
st.caption(
    f"Publication {manifest['publication_id']} • built {manifest['created_at']} • {manifest['dbt_tests_passed']} SQL tests passed"
)
st.warning(
    "Intervals are inferred gaps between reported flights, not directly observed ground-service durations. Delay associations do not establish causation."
)
perf, monthly, aircraft, coverage, health = st.tabs(
    ["Performance", "Monthly airports", "Aircraft timelines", "Coverage & definitions", "Pipeline health"]
)
with perf:
    st.subheader("Daily arrival reliability")
    st.plotly_chart(px.line(daily, x="flight_date", y="ontime_arrival_pct", markers=True), width="stretch")
    airports = table("mart_turnaround_airports")
    minimum = st.number_input("Minimum eligible intervals per airport", min_value=1, value=30, step=10)
    top = airports[airports.eligible_turns >= minimum].sort_values("eligible_turns", ascending=False).head(20)
    left, right = st.columns(2)
    with left:
        st.subheader("Inferred interval by airport")
        st.plotly_chart(
            px.bar(top, x="airport", y="median_actual_turn_min", hover_data=["eligible_turns"]), width="stretch"
        )
    with right:
        st.subheader("Reported delay cause minutes")
        st.plotly_chart(px.bar(table("mart_reported_causes"), x="cause", y="reported_minutes"), width="stretch")
    bands = table("mart_turn_bands").copy()
    n = bands.late_inbound_turns.astype(float)
    k = bands.late_inbound_late_outbound.astype(float)
    fraction = k / n.replace(0, np.nan)
    z = 1.96
    denom = 1 + z * z / n
    center = (fraction + z * z / (2 * n)) / denom
    half = z * np.sqrt(fraction * (1 - fraction) / n + z * z / (4 * n * n)) / denom
    bands["wilson_95_low_pct"] = 100 * (center - half)
    bands["wilson_95_high_pct"] = 100 * (center + half)
    st.subheader("Late outbound given late inbound")
    st.dataframe(bands, width="stretch", hide_index=True)
    st.caption(
        "Wilson intervals are descriptive. Repeated aircraft and shared conditions violate independent sampling assumptions."
    )
with monthly:
    data = table("mart_monthly_airport_performance")
    dims = table("dim_airports")
    choices = sorted(dims.airport.dropna().unique().tolist())
    airport = st.selectbox("Airport", choices, index=choices.index("JFK") if "JFK" in choices else 0)
    selected = data[data.airport_id.isin(dims.loc[dims.airport.eq(airport), "airport_id"])].sort_values("month")
    st.plotly_chart(
        px.line(selected, x="month", y=["ontime_arrival_pct", "cancellation_pct"], markers=True), width="stretch"
    )
    st.dataframe(selected, width="stretch", hide_index=True)
    st.caption("Compare observed coverage days before comparing months. A month label does not guarantee completeness.")
with aircraft:
    path = str(folder / "fact_flights.parquet")
    choices = query(
        path, "SELECT DISTINCT tail_number FROM read_parquet(?) WHERE tail_number IS NOT NULL ORDER BY 1"
    ).tail_number.tolist()
    if choices:
        tail = st.selectbox("Aircraft tail number", choices)
        legs = query(
            path,
            "SELECT flight_number,origin,dest,actual_dep_utc,actual_arr_utc,dep_delay_min,arr_delay_min FROM read_parquet(?) WHERE tail_number=? AND time_valid ORDER BY scheduled_dep_utc LIMIT 1000",
            (tail,),
        )
        st.caption("Up to 1,000 clock-validated flights. Excluded observations remain in the diagnostics below.")
        if not legs.empty:
            legs["route"] = legs.origin + " → " + legs.dest
            st.plotly_chart(
                px.timeline(
                    legs,
                    x_start="actual_dep_utc",
                    x_end="actual_arr_utc",
                    y="route",
                    color="route",
                    hover_data=["flight_number", "dep_delay_min", "arr_delay_min"],
                ),
                width="stretch",
            )
            st.dataframe(legs, width="stretch", hide_index=True)
        links = query(
            str(folder / "int_aircraft_connections.parquet"),
            "SELECT flight_date,previous_flight_id,flight_id,origin,dest,planned_turn_min,actual_turn_min,link_status FROM read_parquet(?) WHERE tail_number=? ORDER BY scheduled_dep_utc LIMIT 1000",
            (tail,),
        )
        st.dataframe(links, width="stretch", hide_index=True)
with coverage:
    st.subheader("Source coverage and revisions")
    st.dataframe(audit, width="stretch", hide_index=True)
    st.subheader("Connection exclusions")
    st.dataframe(table("mart_link_coverage"), width="stretch", hide_index=True)
    st.markdown(
        "**On time:** arrival delay below 15 minutes among noncancelled, nondiverted flights with reported arrival delay. **Eligible interval:** same tail/carrier, matching airport IDs/codes, validated clocks, no overlaps or barriers, both gaps within the configured maximum. **Recovered delay:** inbound arrival delay minus outbound departure delay; can be negative. Sources: BTS records, pinned airportsdata and tzdata. See docs/METRICS.md and docs/DATA_DICTIONARY.md for all formulas and caveats."
    )
with health:
    st.write(
        {
            "publication_created_at": manifest["created_at"],
            "latest_flight_date": str(daily.flight_date.max()),
            "loaded_months": audit.month.tolist(),
            "dbt_tests_passed": manifest["dbt_tests_passed"],
        }
    )
    st.caption(
        "Publication age, source acquisition time and flight-data age are different. The demo is intentionally historical."
    )
    if (root / "active_job.json").exists():
        st.warning("An unfinished job owns this dataset. Inspect it before starting another.")
    jobs = []
    for path in (root / "jobs").glob("*/job.json"):
        try:
            j = json.loads(path.read_text())
            jobs.append(
                {
                    "run_id": j["run_id"],
                    "status": j["status"],
                    "updated_at": j["updated_at"],
                    "months": ", ".join(j["months"]),
                    "error": json.dumps(j.get("error")) if j.get("error") else "",
                }
            )
        except (OSError, ValueError):
            continue
    if jobs:
        st.dataframe(
            pd.DataFrame(jobs).sort_values("updated_at", ascending=False).head(20), width="stretch", hide_index=True
        )
    st.caption(
        "Last successful publication remains available during failures. Local schedules run only while the laptop and Docker are running."
    )

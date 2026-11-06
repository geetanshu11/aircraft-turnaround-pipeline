"""Explicit schema shared by Parquet and PostgreSQL COPY."""

import pyarrow as pa
from .transform import COLUMNS, TEXT, COUNTS

TYPES = {
    name: (
        "date"
        if name == "flight_date"
        else "text"
        if name in TEXT
        else "bigint"
        if name in COUNTS
        else "double precision"
    )
    for name in COLUMNS.values()
}
TYPES["source_row"] = "bigint"
for side in ["origin", "dest"]:
    TYPES.update({side + "_tz": "text", side + "_lat": "double precision", side + "_lon": "double precision"})
for col in ["scheduled_dep_utc", "scheduled_arr_utc", "actual_dep_utc", "actual_arr_utc"]:
    TYPES[col] = "timestamptz"
for col in ["schedule_clock_ok", "actual_clock_ok", "time_valid"]:
    TYPES[col] = "boolean"
TYPES.update(time_status="text", month="text", flight_id="text", record_hash="text")
ARROW_TYPES = {
    "date": pa.date32(),
    "text": pa.string(),
    "bigint": pa.int64(),
    "double precision": pa.float64(),
    "timestamptz": pa.timestamp("us", tz="UTC"),
    "boolean": pa.bool_(),
}
ARROW_SCHEMA = pa.schema([(k, ARROW_TYPES[v]) for k, v in TYPES.items()])

"""Reused legacy UTC reconstruction, with coordinate, overflow and pandas 3 fixes.
Original notebook and source are preserved in notebooks/legacy/.
"""

from __future__ import annotations
import hashlib
import os
from zoneinfo import reset_tzpath, ZoneInfo
import numpy as np
import pandas as pd

# I pin the timezone database rather than inheriting a machine's OS version.
os.environ["PYTHONTZPATH"] = ""
reset_tzpath(())
ZoneInfo.clear_cache()

COLUMNS = {
    "FlightDate": "flight_date",
    "Reporting_Airline": "carrier",
    "DOT_ID_Reporting_Airline": "carrier_id",
    "Tail_Number": "tail_number",
    "Flight_Number_Reporting_Airline": "flight_number",
    "OriginAirportID": "origin_id",
    "Origin": "origin",
    "OriginCityName": "origin_city",
    "DestAirportID": "dest_id",
    "Dest": "dest",
    "DestCityName": "dest_city",
    "CRSDepTime": "scheduled_dep_hhmm",
    "CRSArrTime": "scheduled_arr_hhmm",
    "DepTime": "actual_dep_hhmm",
    "ArrTime": "actual_arr_hhmm",
    "DepDelay": "dep_delay_min",
    "ArrDelay": "arr_delay_min",
    "CRSElapsedTime": "scheduled_elapsed_min",
    "ActualElapsedTime": "actual_elapsed_min",
    "Cancelled": "cancelled",
    "CancellationCode": "cancellation_code",
    "Diverted": "diverted",
    "TaxiOut": "taxi_out_min",
    "TaxiIn": "taxi_in_min",
    "Distance": "distance_miles",
    "CarrierDelay": "carrier_delay_min",
    "WeatherDelay": "weather_delay_min",
    "NASDelay": "nas_delay_min",
    "SecurityDelay": "security_delay_min",
    "LateAircraftDelay": "late_aircraft_delay_min",
}
KEY = ["flight_date", "carrier_id", "flight_number", "origin_id", "dest_id", "scheduled_dep_hhmm"]
TEXT = {"flight_date", "carrier", "tail_number", "origin", "dest", "origin_city", "dest_city", "cancellation_code"}
NUMERIC = set(COLUMNS.values()) - TEXT
COUNTS = {
    "carrier_id",
    "flight_number",
    "origin_id",
    "dest_id",
    "cancelled",
    "diverted",
    "scheduled_dep_hhmm",
    "scheduled_arr_hhmm",
    "actual_dep_hhmm",
    "actual_arr_hhmm",
}


def clock_minutes(s):
    n = pd.to_numeric(s, errors="coerce")
    valid = n.notna() & (n % 1 == 0) & (((n >= 0) & (n <= 2359) & (n % 100 < 60)) | (n == 2400))
    return ((n // 100) * 60 + n % 100).where(valid)


def utc_schedule(dates, clocks, zones):
    minutes = clock_minutes(clocks)
    local = pd.to_datetime(dates, errors="coerce") + pd.to_timedelta(minutes, unit="m")
    result = pd.Series(pd.NaT, index=dates.index, dtype="datetime64[ns, UTC]")
    for zone in zones.dropna().unique():
        ix = zones.eq(zone)
        # I leave ambiguous and nonexistent local times unresolved.
        result.loc[ix] = (
            local.loc[ix].dt.tz_localize(ZoneInfo(zone), ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")
        )
    return result


def local_clock(utc, zones):
    result = pd.Series(np.nan, index=utc.index)
    for zone in zones.dropna().unique():
        ix = zones.eq(zone)
        local = utc.loc[ix].dt.tz_convert(ZoneInfo(zone))
        result.loc[ix] = local.dt.hour * 60 + local.dt.minute
    return result


def same_clock(utc, zones, original):
    expected = clock_minutes(original) % 1440
    observed = local_clock(utc, zones)
    diff = (observed - expected).abs()
    return expected.notna() & observed.notna() & ((diff <= 1) | (diff >= 1439))


def normalize(raw, month, airports, source_row_start=1):
    missing = set(COLUMNS) - set(raw.columns)
    if missing:
        raise ValueError(f"BTS schema missing columns: {sorted(missing)}")
    d = raw[list(COLUMNS)].rename(columns=COLUMNS).reset_index(drop=True).copy()
    d["source_row"] = np.arange(source_row_start, source_row_start + len(d), dtype=np.int64)
    reasons = pd.Series("", index=d.index)

    def bad(mask, label):
        mask = mask.fillna(False)
        reasons.loc[mask] += label + ";"

    for col in TEXT:
        d[col] = d[col].astype("string").fillna("").str.strip()
    for col in NUMERIC:
        original = d[col].astype("string").fillna("").str.strip()
        d[col] = pd.to_numeric(original, errors="coerce").astype(float)
        bad(original.ne("") & (d[col].isna() | ~np.isfinite(d[col])), "invalid_numeric:" + col)
        if col in COUNTS:
            bad(d[col].notna() & (d[col] % 1 != 0), "non_integer:" + col)
    for col in ["dep_delay_min", "arr_delay_min", "scheduled_elapsed_min", "actual_elapsed_min"]:
        bad(d[col].abs().gt(60 * 24 * 31), "implausible_duration:" + col)
    date = pd.to_datetime(d.flight_date, format="%Y-%m-%d", errors="coerce")
    bad(date.isna() | date.dt.strftime("%Y-%m").ne(month), "invalid_partition_date")
    for col in ["carrier", "origin", "dest"]:
        bad(d[col].eq(""), "missing:" + col)
    for col in ["carrier_id", "flight_number", "origin_id", "dest_id"]:
        bad(d[col].isna() | d[col].le(0), "invalid_key:" + col)
    for col in ["cancelled", "diverted"]:
        bad(~d[col].isin([0.0, 1.0]), "invalid_flag:" + col)
    bad(d.cancelled.eq(1) & d.diverted.eq(1), "conflicting_status")
    bad(clock_minutes(d.scheduled_dep_hhmm).isna(), "invalid_scheduled_departure")
    for col in [
        "scheduled_elapsed_min",
        "actual_elapsed_min",
        "distance_miles",
        "taxi_in_min",
        "taxi_out_min",
        "carrier_delay_min",
        "weather_delay_min",
        "nas_delay_min",
        "security_delay_min",
        "late_aircraft_delay_min",
    ]:
        bad(d[col].lt(0), "negative:" + col)
    rejected = raw.reset_index(drop=True).loc[reasons.ne("")].copy()
    rejected["source_row"] = d.loc[reasons.ne(""), "source_row"].values
    rejected["reject_reason"] = reasons.loc[reasons.ne("")].values
    d = d.loc[reasons.eq("")].copy()
    d["flight_date"] = pd.to_datetime(d.flight_date)
    for col in COUNTS:
        d[col] = d[col].astype("Int64")
    for col in ["carrier", "origin", "dest", "tail_number"]:
        d[col] = d[col].str.upper()
    d["tail_number"] = d.tail_number.replace({"": pd.NA, "UNKNOWN": pd.NA, "UNKNOW": pd.NA})
    ref = airports.set_index("iata")
    for side in ["origin", "dest"]:
        for field in ["tz", "lat", "lon"]:
            d[side + "_" + field] = d[side].map(ref[field])
    for side in ["origin", "dest"]:
        invalid = (d[side + "_lat"].notna() & ~d[side + "_lat"].between(-90, 90)) | (
            d[side + "_lon"].notna() & ~d[side + "_lon"].between(-180, 180)
        )
        if invalid.any():
            raise ValueError("Invalid airport reference coordinates")
    d["scheduled_dep_utc"] = utc_schedule(d.flight_date, d.scheduled_dep_hhmm, d.origin_tz)
    d["scheduled_arr_utc"] = d.scheduled_dep_utc + pd.to_timedelta(d.scheduled_elapsed_min, unit="m")
    d["schedule_clock_ok"] = same_clock(d.scheduled_arr_utc, d.dest_tz, d.scheduled_arr_hhmm)
    completed = d.cancelled.eq(0) & d.diverted.eq(0)
    d["actual_dep_utc"] = (d.scheduled_dep_utc + pd.to_timedelta(d.dep_delay_min, unit="m")).where(completed)
    d["actual_arr_utc"] = (d.actual_dep_utc + pd.to_timedelta(d.actual_elapsed_min, unit="m")).where(completed)
    arrival_delay = (d.actual_arr_utc - d.scheduled_arr_utc).dt.total_seconds() / 60
    d["actual_clock_ok"] = (
        same_clock(d.actual_dep_utc, d.origin_tz, d.actual_dep_hhmm)
        & same_clock(d.actual_arr_utc, d.dest_tz, d.actual_arr_hhmm)
        & arrival_delay.sub(d.arr_delay_min).abs().le(1)
    )
    d["time_valid"] = (
        completed & d.schedule_clock_ok & d.actual_clock_ok & d.actual_elapsed_min.gt(0) & d.scheduled_elapsed_min.gt(0)
    )
    d["time_status"] = np.select(
        [
            d.cancelled.eq(1).to_numpy(bool),
            d.diverted.eq(1).to_numpy(bool),
            (d.origin_tz.isna() | d.dest_tz.isna()).to_numpy(bool),
            d.scheduled_dep_utc.isna().to_numpy(bool),
            ~d.schedule_clock_ok.to_numpy(bool),
            ~d.actual_clock_ok.to_numpy(bool),
        ],
        [
            "cancelled",
            "diverted",
            "missing_timezone",
            "unresolved_local_time",
            "schedule_clock_mismatch",
            "actual_clock_mismatch",
        ],
        default="valid",
    )
    d.loc[~d.time_valid & d.time_status.eq("valid"), "time_status"] = "invalid_elapsed"
    d["month"] = month
    if d.empty:
        d["flight_id"] = pd.Series(dtype="string")
        d["record_hash"] = pd.Series(dtype="string")
        return d.reset_index(drop=True), rejected.reset_index(drop=True)
    # Explicit legacy string representation prevents pandas 3 nullable-string
    # behavior from changing stable business identifiers and source hashes.
    canonical = pd.DataFrame({c: d[c].astype(object).map(str) for c in COLUMNS.values()}, index=d.index)
    canonical["flight_date"] = d.flight_date.dt.strftime("%Y-%m-%d")
    key_text = canonical[KEY].agg("|".join, axis=1)
    d["flight_id"] = key_text.map(lambda x: hashlib.sha256(x.encode()).hexdigest())
    # This hash describes source facts; derived timestamps also depend on the pinned reference.
    source_text = canonical[list(COLUMNS.values())].agg("|".join, axis=1)
    d["record_hash"] = source_text.map(lambda x: hashlib.sha256(x.encode()).hexdigest())
    return d.reset_index(drop=True), rejected.reset_index(drop=True)

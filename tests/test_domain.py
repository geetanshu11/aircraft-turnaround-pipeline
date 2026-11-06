import pandas as pd
import pytest
from turnaround.transform import normalize, clock_minutes, utc_schedule
from .helpers import row


@pytest.mark.parametrize(
    "hhmm,expected",
    [
        ("0000", 0),
        ("2359", 1439),
        ("2400", 1440),
        ("1259", 779),
        ("1260", None),
        ("2401", None),
        ("-1", None),
        ("1.5", None),
        ("", None),
    ],
)
def test_hhmm_contract(hhmm, expected):
    v = clock_minutes(pd.Series([hhmm])).iloc[0]
    assert pd.isna(v) if expected is None else v == expected


@pytest.mark.parametrize(
    "date,clock,expected",
    [
        ("2025-03-08", "1200", "2025-03-08T17:00:00Z"),
        ("2025-03-09", "1200", "2025-03-09T16:00:00Z"),
        ("2025-03-09", "0230", None),
        ("2025-11-02", "0130", None),
        ("2025-03-10", "2400", "2025-03-11T04:00:00Z"),
    ],
)
def test_timezone_dst_and_midnight(date, clock, expected):
    v = utc_schedule(pd.Series([date]), pd.Series([clock]), pd.Series(["America/New_York"])).iloc[0]
    assert pd.isna(v) if expected is None else v == pd.Timestamp(expected)


def test_real_clock_reconstruction(airports):
    d, b = normalize(pd.DataFrame([row()]), "2025-03", airports)
    assert len(b) == 0 and bool(d.time_valid.iloc[0])
    assert d.actual_dep_utc.iloc[0] == pd.Timestamp("2025-03-10T12:20:00Z")


def test_cross_zone_overnight_and_multiday_delay(airports):
    r = row(
        Origin="LAX",
        Dest="JFK",
        CRSDepTime="2300",
        CRSArrTime="0700",
        CRSElapsedTime="300",
        DepTime="0100",
        ArrTime="0900",
        DepDelay="1560",
        ArrDelay="1560",
        ActualElapsedTime="300",
    )
    d, _ = normalize(pd.DataFrame([r]), "2025-03", airports)
    assert bool(d.time_valid.iloc[0]) and d.actual_arr_utc.iloc[0] == pd.Timestamp("2025-03-12T13:00:00Z")


@pytest.mark.parametrize(
    "field,value",
    [
        ("FlightDate", "2025-02-28"),
        ("Cancelled", "2"),
        ("Flight_Number_Reporting_Airline", ""),
        ("Distance", "-10"),
        ("DepDelay", "oops"),
        ("Tail_Number", None),
        ("Origin", ""),
    ],
)
def test_contract_rejection_and_nullable_tail(airports, field, value):
    d, b = normalize(pd.DataFrame([row(**{field: value})]), "2025-03", airports)
    if field == "Tail_Number":
        assert len(d) == 1 and pd.isna(d.tail_number.iloc[0])
    else:
        assert len(b) == 1 and len(d) == 0


def test_unknown_timezone_retained_but_unlinked(airports):
    d, _ = normalize(pd.DataFrame([row(Origin="ZZZ")]), "2025-03", airports)
    assert len(d) == 1 and not bool(d.time_valid.iloc[0]) and d.time_status.iloc[0] == "missing_timezone"


def test_clock_mismatch_is_not_silently_corrected(airports):
    d, _ = normalize(pd.DataFrame([row(ArrTime="0950")]), "2025-03", airports)
    assert not bool(d.time_valid.iloc[0])


def test_missing_delay_does_not_become_zero(airports):
    d, _ = normalize(pd.DataFrame([row(DepDelay="")]), "2025-03", airports)
    assert pd.isna(d.actual_dep_utc.iloc[0]) and pd.isna(d.dep_delay_min.iloc[0])


def test_required_schema(airports):
    with pytest.raises(ValueError, match="schema"):
        normalize(pd.DataFrame([row()]).drop(columns="ArrDelay"), "2025-03", airports)

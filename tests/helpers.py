import pandas as pd
import zipfile
from turnaround.transform import COLUMNS


def row(**changes):
    r = {k: "" for k in COLUMNS}
    r.update(
        FlightDate="2025-03-10",
        Reporting_Airline="AA",
        DOT_ID_Reporting_Airline="19805",
        Tail_Number="N123AA",
        Flight_Number_Reporting_Airline="101",
        OriginAirportID="10721",
        Origin="BOS",
        OriginCityName="Boston, MA",
        DestAirportID="12478",
        Dest="JFK",
        DestCityName="New York, NY",
        CRSDepTime="0800",
        CRSArrTime="0900",
        DepTime="0820",
        ArrTime="0920",
        DepDelay="20",
        ArrDelay="20",
        CRSElapsedTime="60",
        ActualElapsedTime="60",
        Cancelled="0",
        Diverted="0",
        Distance="187",
        TaxiOut="10",
        TaxiIn="5",
    )
    r.update(changes)
    return r


def second(**changes):
    r = row(
        Flight_Number_Reporting_Airline="102",
        OriginAirportID="12478",
        Origin="JFK",
        OriginCityName="New York, NY",
        DestAirportID="10721",
        Dest="BOS",
        DestCityName="Boston, MA",
        CRSDepTime="1000",
        CRSArrTime="1100",
        DepTime="1010",
        ArrTime="1110",
        DepDelay="10",
        ArrDelay="10",
    )
    r.update(changes)
    return r


def zip_rows(tmp_path, rows, name="source.zip"):
    path = tmp_path / name
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        info = zipfile.ZipInfo("source.csv", date_time=(2025, 1, 1, 0, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        z.writestr(info, pd.DataFrame(rows).to_csv(index=False))
    return path

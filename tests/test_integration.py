from dataclasses import replace
import pytest
from turnaround import pipeline
from turnaround.core import Config, read_json, digest, write_json
from turnaround.warehouse import connect
from .helpers import row, second, zip_rows

pytestmark = [pytest.mark.postgres, pytest.mark.integration]


def inputs(tmp_path, revision=False):
    a = row(
        FlightDate="2025-01-31",
        CRSDepTime="2300",
        CRSArrTime="2400",
        DepTime="2320",
        ArrTime="0020",
        Tail_Number="N999AA" if revision else "N123AA",
    )
    b = second(FlightDate="2025-02-01", CRSDepTime="0100", CRSArrTime="0200", DepTime="0110", ArrTime="0210")
    jan = (
        [a]
        if revision
        else [a, row(FlightDate="2025-01-31", Tail_Number="N777AA", Flight_Number_Reporting_Airline="999")]
    )
    return {
        "2025-01": {"path": str(zip_rows(tmp_path, jan, "jan-rev.zip" if revision else "jan.zip"))},
        "2025-02": {"path": str(zip_rows(tmp_path, [b], "feb.zip"))},
    }


def query(cfg, sql):
    with connect(cfg) as con:
        return list(con.execute(sql))


def tables(cfg):
    return {
        n: query(cfg, f"SELECT * FROM analytics.{n} ORDER BY 1,2")
        for n in ["fact_flights", "fact_turnarounds", "mart_daily_reliability"]
    }


def test_e2e_cross_month_revision_idempotency_and_replay(pg, tmp_path):
    a = pipeline.run_pipeline(pg, ["2025-01", "2025-02"], inputs(tmp_path), "first")
    turns = query(pg, "SELECT * FROM analytics.fact_turnarounds")
    assert len(turns) == 1 and float(turns[0]["planned_turn_min"]) == 60 and float(turns[0]["actual_turn_min"]) == 50
    assert a["quality_test_count"] > 40
    b = pipeline.run_pipeline(pg, ["2025-01", "2025-02"], inputs(tmp_path), "second")
    assert a["publication"]["publication_id"] == b["publication"]["publication_id"] and all(
        r["status"] == "unchanged" for r in b["loaded"]
    )
    path = pg.root / "jobs" / digest("second")[:24] / "job.json"
    write_json(pg.root / "active_job.json", {"run_id": "second", "job": str(path)})
    pipeline.resume(path)
    assert not (pg.root / "active_job.json").exists()
    c = pipeline.run_pipeline(pg, ["2025-01"], {"2025-01": inputs(tmp_path, True)["2025-01"]}, "revision")
    assert c["loaded"][0]["updated"] == 1 and c["loaded"][0]["withdrawn"] == 1
    assert query(pg, "SELECT count(*) n FROM analytics.fact_flights")[0]["n"] == 2
    assert query(pg, "SELECT count(*) n FROM analytics.fact_turnarounds")[0]["n"] == 0
    before = tables(pg)
    committed = query(
        pg, "SELECT c.month,v.manifest FROM meta.checkpoints c JOIN meta.partition_versions v USING(version_id)"
    )
    replay = {
        r["month"]: {"path": str(pg.root / r["manifest"]["raw_relative_path"]), "metadata": r["manifest"]}
        for r in committed
    }
    with connect(pg, autocommit=True) as con:
        con.execute("DROP SCHEMA analytics CASCADE; DROP SCHEMA raw CASCADE; DROP SCHEMA meta CASCADE")
    new = Config(tmp_path / "rebuild", mode="fixture", chunk_rows=2)
    pipeline.run_pipeline(new, list(replay), replay, "rebuild")
    assert tables(new) == before


def test_failed_quality_keeps_last_publication_and_resumes(pg, tmp_path, monkeypatch):
    first = pipeline.run_pipeline(pg, ["2025-01", "2025-02"], inputs(tmp_path), "first")
    pointer = read_json(pg.root / "gold/latest.json")
    path = pipeline.create_job(pg, ["2025-01"], run_id="retry", inputs={"2025-01": inputs(tmp_path, True)["2025-01"]})
    original = pipeline._quality_tests

    def fail(*a):
        raise RuntimeError("Injected quality failure")

    monkeypatch.setattr(pipeline, "_quality_tests", fail)
    with pytest.raises(RuntimeError, match="Injected"):
        pipeline.resume(path)
    assert read_json(pg.root / "gold/latest.json") == pointer and read_json(path)["status"] == "failed"
    with pytest.raises(RuntimeError, match="owns"):
        pipeline.create_job(pg, ["2025-01"], run_id="competing")
    monkeypatch.setattr(pipeline, "_quality_tests", original)
    r = pipeline.resume(path)
    assert r["status"] == "succeeded" and not (pg.root / "active_job.json").exists()
    assert r["publication"]["publication_id"] != first["publication"]["publication_id"]


def test_nonoperating_legs_and_quarantine_are_sequence_barriers(pg, tmp_path):
    rows = []
    for i, changes in enumerate([{"Cancelled": "1"}, {"Diverted": "1"}, {"ArrTime": "0955"}, {"Distance": "-1"}]):
        tail = f"N{i}XX"
        a = row(Tail_Number=tail, Flight_Number_Reporting_Airline=str(i * 3 + 1))
        b = row(
            Tail_Number=tail,
            Flight_Number_Reporting_Airline=str(i * 3 + 2),
            CRSDepTime="0900",
            CRSArrTime="1000",
            DepTime="0920",
            **({"ArrTime": "1020"} | changes),
        )
        c = second(Tail_Number=tail, Flight_Number_Reporting_Airline=str(i * 3 + 3))
        rows.extend([a, b, c])
    cfg = replace(pg, reject_fraction=0.1)
    pipeline.run_pipeline(cfg, ["2025-03"], {"2025-03": {"path": str(zip_rows(tmp_path, rows))}}, "barriers")
    assert query(cfg, "SELECT count(*) n FROM analytics.fact_turnarounds")[0]["n"] == 0
    statuses = {
        r["link_status"] for r in query(cfg, "SELECT distinct link_status FROM analytics.int_aircraft_connections")
    }
    assert {"cancelled_leg", "diverted_leg", "invalid_clock", "quarantined_movement"} <= statuses


def test_sql_interval_exclusions_and_maximum_prior_arrival(pg, tmp_path):
    cases = [
        ("airport_discontinuity", {}, dict(Origin="BOS", OriginAirportID="10721")),
        ("schedule_overlap", {}, dict(CRSDepTime="0830", CRSArrTime="0930", DepTime="0840", ArrTime="0940")),
        ("actual_overlap", dict(DepTime="0930", ArrTime="1030", DepDelay="90", ArrDelay="90"), {}),
        ("long_gap", {}, dict(CRSDepTime="2300", CRSArrTime="2400", DepTime="2310", ArrTime="0010")),
        ("carrier_changed", {}, dict(Reporting_Airline="DL", DOT_ID_Reporting_Airline="19790")),
    ]
    rows = []
    expected = {}
    for i, (status, inbound, outbound) in enumerate(cases):
        a = row(**inbound)
        b = second(**outbound)
        a.update(Tail_Number=f"N{i}TEST", Flight_Number_Reporting_Airline=str(200 + i * 2))
        b.update(Tail_Number=f"N{i}TEST", Flight_Number_Reporting_Airline=str(201 + i * 2))
        rows.extend([a, b])
        expected[201 + i * 2] = status
    rows.extend(
        [
            row(
                Tail_Number="NPRIOR",
                Flight_Number_Reporting_Airline="300",
                DepTime="1200",
                ArrTime="1300",
                DepDelay="240",
                ArrDelay="240",
            ),
            second(Tail_Number="NPRIOR", Flight_Number_Reporting_Airline="301"),
            row(
                Tail_Number="NPRIOR",
                Flight_Number_Reporting_Airline="302",
                CRSDepTime="1200",
                CRSArrTime="1300",
                DepTime="1200",
                ArrTime="1300",
                DepDelay="0",
                ArrDelay="0",
            ),
            row(Tail_Number="", Flight_Number_Reporting_Airline="400"),
        ]
    )
    expected[301] = "actual_overlap"
    expected[302] = "actual_overlap"
    pipeline.run_pipeline(pg, ["2025-03"], {"2025-03": {"path": str(zip_rows(tmp_path, rows))}}, "intervals")
    actual = {
        r["flight_number"]: r["link_status"]
        for r in query(pg, "SELECT flight_number,link_status FROM analytics.int_aircraft_connections")
    }
    assert all(actual[k] == v for k, v in expected.items()), actual
    assert 400 not in actual and query(pg, "SELECT count(*) n FROM analytics.fact_turnarounds")[0]["n"] == 0

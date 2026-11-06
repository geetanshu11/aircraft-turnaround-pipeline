"""Offline HTML explorer and versioned, portable analytical exports."""
from __future__ import annotations
from pathlib import Path
import html
import json
import math
import os
import shutil
import uuid
import pandas as pd
from plotly.offline import get_plotlyjs
from aircraft_core import BASE, connect, digest, now, sql_path, write_json
from aircraft_models import EXPORTS, metrics


def records(con, query):
    # Pandas converts database nulls to JSON null and timestamps to ISO strings here.
    return json.loads(con.execute(query).fetchdf().to_json(orient="records", date_format="iso"))


def report_payload(cfg):
    con = connect(cfg)
    try:
        m = metrics(cfg)
        airports = records(con, """SELECT a.airport,a.city,a.latitude,a.longitude,
          sum(d.scheduled_departures) AS flights,sum(d.cancellations) AS cancellations,
          sum(d.measured_departures) AS measured_departures,sum(d.late_departures) AS late_departures
          FROM mart_airport_daily d JOIN dim_airport a USING(airport)
          GROUP BY ALL ORDER BY flights DESC""")
        turns = records(con, "SELECT * FROM mart_turnaround_airports ORDER BY eligible_turns DESC")
        daily = records(con, "SELECT * FROM mart_daily_reliability ORDER BY flight_date")
        airport_daily = records(con, "SELECT * FROM mart_airport_daily ORDER BY flight_date,airport")
        bands = records(con, "SELECT * FROM mart_turn_bands ORDER BY CASE planned_turn_band WHEN '<45 min' THEN 1 WHEN '45–59 min' THEN 2 WHEN '60–89 min' THEN 3 WHEN '90–119 min' THEN 4 ELSE 5 END")
        for b in bands:
            n, successes = b["late_inbound_turns"], b["late_inbound_late_outbound"]
            if n:
                p = successes/n; z = 1.959963984540054; den=1+z*z/n
                center=(p+z*z/(2*n))/den; half=z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))/den
                b["wilson_low_pct"], b["wilson_high_pct"] = 100*(center-half),100*(center+half)
            else:
                b["wilson_low_pct"]=b["wilson_high_pct"]=None
        examples = records(con, """SELECT tail_number,flight_date::VARCHAR AS day,count(*) AS legs,
           avg(dep_delay_min) AS average_delay FROM flights WHERE time_valid AND tail_number IS NOT NULL
           GROUP BY tail_number,flight_date HAVING count(*) BETWEEN 5 AND 10
           ORDER BY avg(dep_delay_min) DESC,tail_number,flight_date LIMIT 24""")
        if not examples:
            examples = records(con, "SELECT tail_number,flight_date::VARCHAR AS day,count(*) AS legs FROM flights WHERE tail_number IS NOT NULL GROUP BY tail_number,flight_date ORDER BY count(*) DESC LIMIT 5")
        journeys=[]
        for e in examples:
            df=con.execute("""SELECT f.flight_id,f.carrier,f.flight_number,f.origin,f.dest,f.cancelled,f.diverted,
                f.scheduled_dep_utc,f.scheduled_arr_utc,f.actual_dep_utc,f.actual_arr_utc,
                f.dep_delay_min,f.arr_delay_min,f.time_status,r.link_status,
                r.planned_turn_min,r.actual_turn_min
                FROM flights f LEFT JOIN rotation_candidates r USING(flight_id)
                WHERE f.tail_number=? AND f.flight_date=? ORDER BY f.scheduled_dep_utc,f.flight_id""",[e["tail_number"],e["day"]]).fetchdf()
            journeys.append({**e,"flights":json.loads(df.to_json(orient="records",date_format="iso"))})
        return {"metrics":m,"generated_at":now(),"airports":airports,"turns":turns,"daily":daily,
                "airport_daily":airport_daily,"bands":bands,"journeys":journeys,
                "routes":records(con,"SELECT * FROM mart_routes ORDER BY scheduled_flights DESC"),
                "causes":records(con,"SELECT * FROM mart_reported_causes ORDER BY reported_minutes DESC"),
                "coverage":records(con,"SELECT * FROM mart_link_coverage ORDER BY candidate_records DESC"),
                "sources":records(con,"SELECT v.partition_month,v.raw_rows,v.accepted_rows,v.source_sha256,v.reference_hash,v.received_at FROM checkpoints c JOIN partition_versions v USING(version_id) ORDER BY v.partition_month"),
                "geometry":json.loads((BASE/"assets/north_america.geojson").read_text())}
    finally:
        con.close()


def write_report(cfg, path):
    payload = report_payload(cfg)
    body = (BASE/"assets/report.html").read_text()
    # Escaping '<' keeps source strings from ending the JSON script element.
    data = json.dumps(payload,allow_nan=False,default=lambda x:x.item() if hasattr(x,"item") else str(x)).replace("<","\\u003c")
    body=body.replace("__PAYLOAD__",data).replace("__PLOTLY__",get_plotlyjs())
    Path(path).write_text(body,encoding="utf-8")
    return payload["metrics"]


def publish(cfg):
    m=metrics(cfg)
    con=connect(cfg)
    versions=con.execute("SELECT partition_month,version_id FROM checkpoints ORDER BY partition_month").fetchall()
    publication=digest({"versions":versions,"max_turn":cfg.max_turn_minutes,"mode":cfg.mode})[:16]+"-"+uuid.uuid4().hex[:8]
    destination=cfg.root/"gold"/publication
    staging=cfg.root/"gold"/(publication+".partial")
    if staging.exists():shutil.rmtree(staging)
    staging.mkdir()
    try:
        for table in EXPORTS:
            con.execute(f"COPY {table} TO {sql_path(staging/(table+'.parquet'))} (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.execute(f"COPY mart_turnaround_airports TO {sql_path(staging/'airport_turnarounds.csv')} (HEADER, DELIMITER ',')")
        write_json(staging/"metrics.json",m)
        write_json(staging/"integrity_checks.json",m["checks"])
        write_report(cfg,staging/"report.html")
        write_json(staging/"publication.json",{"publication_id":publication,"created_at":now(),"versions":versions,
                   "max_turn_minutes":cfg.max_turn_minutes,"mode":cfg.mode})
        if destination.exists():shutil.rmtree(destination)
        os.replace(staging,destination)
        con.execute("INSERT OR REPLACE INTO publications VALUES (?,?,?,?,?)",[publication,now(),json.dumps({"max_turn_minutes":cfg.max_turn_minutes,"mode":cfg.mode}),json.dumps(m,default=str),str(destination.relative_to(cfg.root))])
        write_json(cfg.root/"gold/latest.json",{"publication_id":publication,"relative_path":str(destination.relative_to(cfg.root))})
        return {"publication_id":publication,"directory":str(destination),"report":str(destination/"report.html"),"metrics":m}
    finally:
        con.close()

"""Warehouse build, engineering checks, and reproducible SQL metrics."""
import math
from aircraft_core import BASE, connect, now, sql_path

EXPORTS = ["dim_airport", "dim_carrier", "dim_date", "fact_turnarounds", "mart_daily_reliability",
           "mart_airport_daily", "mart_routes", "mart_turnaround_airports", "mart_turn_bands",
           "mart_reported_causes", "mart_link_coverage", "partition_versions", "quality_checks"]


def build_models(cfg):
    con = connect(cfg)
    try:
        con.execute("BEGIN")
        con.execute((BASE / "sql/models.sql").read_text().format(max_turn=int(cfg.max_turn_minutes)))
        checks = warehouse_checks(con, cfg)
        if any(v != 0 for v in checks.values()):
            raise ValueError(f"Warehouse integrity checks failed: {checks}")
        con.execute("COMMIT")
        return checks
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def warehouse_checks(con, cfg):
    queries = {
        "duplicate_flight_ids": "SELECT count(*)-count(DISTINCT flight_id) FROM flights",
        "duplicate_outbound_links": "SELECT count(*)-count(DISTINCT outbound_flight_id) FROM fact_turnarounds",
        "duplicate_inbound_links": "SELECT count(*)-count(DISTINCT inbound_flight_id) FROM fact_turnarounds",
        "orphan_turnarounds": "SELECT count(*) FROM fact_turnarounds t LEFT JOIN flights i ON t.inbound_flight_id=i.flight_id LEFT JOIN flights o ON t.outbound_flight_id=o.flight_id WHERE i.flight_id IS NULL OR o.flight_id IS NULL",
        "airport_mismatch": "SELECT count(*) FROM fact_turnarounds t JOIN flights i ON t.inbound_flight_id=i.flight_id JOIN flights o ON t.outbound_flight_id=o.flight_id WHERE i.dest<>o.origin",
        "invalid_linked_times": "SELECT count(*) FROM fact_turnarounds t JOIN flights i ON t.inbound_flight_id=i.flight_id JOIN flights o ON t.outbound_flight_id=o.flight_id WHERE NOT i.time_valid OR NOT o.time_valid",
        "invalid_turn_ranges": f"SELECT count(*) FROM fact_turnarounds WHERE actual_turn_min<0 OR planned_turn_min<0 OR actual_turn_min>{int(cfg.max_turn_minutes)} OR planned_turn_min>{int(cfg.max_turn_minutes)}",
        "turn_identity_errors": "SELECT count(*) FROM fact_turnarounds WHERE abs((actual_turn_min-planned_turn_min)-added_delay_min)>1",
        "daily_reconciliation_errors": "SELECT abs((SELECT count(*) FROM flights)-(SELECT sum(scheduled_flights) FROM mart_daily_reliability))",
        "checkpoint_reconciliation_errors": "SELECT count(*) FROM checkpoints c JOIN partition_versions v USING(version_id) WHERE v.accepted_rows<>(SELECT count(*) FROM flights f WHERE f.month=c.partition_month)",
    }
    return {name: int(con.execute(query).fetchone()[0]) for name, query in queries.items()}


def metrics(cfg):
    con = connect(cfg)
    try:
        row = con.execute("""SELECT count(*) AS flights, min(flight_date)::VARCHAR AS start_date,
         max(flight_date)::VARCHAR AS end_date,count(DISTINCT tail_number) AS aircraft,
         count(DISTINCT carrier_id) AS carriers, sum(cancelled)::BIGINT AS cancellations,
         sum(diverted)::BIGINT AS diversions,count(*) FILTER(WHERE time_valid) AS clock_valid_flights,
         count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min IS NOT NULL) AS measured_arrivals,
         count(*) FILTER(WHERE cancelled=0 AND diverted=0 AND arr_delay_min<15) AS ontime_arrivals
         FROM flights""").fetchdf().iloc[0].to_dict()
        row.update(con.execute("""SELECT count(*) AS eligible_turns,
          count(*) FILTER(WHERE inbound_late) AS late_inbound_turns,
          count(*) FILTER(WHERE inbound_late AND outbound_late) AS late_inbound_late_outbound,
          count(*) FILTER(WHERE inbound_late AND NOT outbound_late) AS recovered_to_ontime,
          median(actual_turn_min) AS median_actual_turn_min FROM fact_turnarounds""").fetchdf().iloc[0].to_dict())
        row["airports"] = con.execute("SELECT count(*) FROM dim_airport").fetchone()[0]
        row["ontime_arrival_pct"] = 100 * row["ontime_arrivals"] / row["measured_arrivals"] if row["measured_arrivals"] else None
        row["conditional_outbound_late_pct"] = 100 * row["late_inbound_late_outbound"] / row["late_inbound_turns"] if row["late_inbound_turns"] else None
        row["mode"] = cfg.mode
        row["max_turn_minutes"] = cfg.max_turn_minutes
        row["checks"] = warehouse_checks(con, cfg)
        # An empty eligible-pair cohort has no median; it is not a zero-minute turn.
        row = {k: (None if isinstance(v, float) and not math.isfinite(v) else v) for k,v in row.items()}
        return row
    finally:
        con.close()

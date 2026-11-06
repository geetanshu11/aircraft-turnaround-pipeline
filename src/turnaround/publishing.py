"""Stream tested products to immutable analytical snapshots; promote the pointer last."""

from decimal import Decimal
import os
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import sql
from psycopg.types.json import Jsonb
from .core import digest, model_hash, now, read_json, sha256, write_json
from .warehouse import active_versions, connect, signature

EXPORTS = [
    "fact_flights",
    "int_aircraft_connections",
    "fact_turnarounds",
    "fact_disruptions",
    "dim_airports",
    "dim_carriers",
    "dim_dates",
    "mart_daily_reliability",
    "mart_airport_daily",
    "mart_routes",
    "mart_turnaround_airports",
    "mart_turn_bands",
    "mart_reported_causes",
    "mart_link_coverage",
    "mart_monthly_airport_performance",
    "mart_pipeline_audit",
]


def _arrow_type(oid):
    return {
        16: pa.bool_(),
        20: pa.int64(),
        21: pa.int64(),
        23: pa.int64(),
        700: pa.float64(),
        701: pa.float64(),
        1700: pa.float64(),
        1082: pa.date32(),
        1114: pa.timestamp("us"),
        1184: pa.timestamp("us", tz="UTC"),
    }.get(oid, pa.string())


def export_table(con, name, path):
    with con.cursor(name="export_" + name) as cur:
        cur.execute(sql.SQL("SELECT * FROM analytics.{}").format(sql.Identifier(name)))
        rows = cur.fetchmany(10_000)
        schema = pa.schema([(c.name, _arrow_type(c.type_code)) for c in cur.description])
        total = 0
        with pq.ParquetWriter(path, schema, compression="zstd") as writer:
            while rows:
                clean = [{k: float(v) if isinstance(v, Decimal) else v for k, v in r.items()} for r in rows]
                writer.write_table(pa.Table.from_pylist(clean, schema=schema))
                total += len(rows)
                rows = cur.fetchmany(10_000)
    return {"rows": total, "sha256": sha256(path), "bytes": path.stat().st_size}


def publish(cfg, job):
    if not job.get("quality_signature") or not job.get("quality_test_count"):
        raise ValueError("Publication requires successful SQL quality tests")
    code_hash = model_hash()
    if code_hash != job["model_hash"]:
        raise ValueError("dbt definitions changed after tests")
    publication = digest(
        {"signature": job["quality_signature"], "max_turn_minutes": cfg.max_turn_minutes, "models": code_hash}
    )[:24]
    dest = cfg.root / "gold" / publication
    stage = cfg.root / "gold" / (publication + ".partial")
    with connect(cfg) as con:
        con.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        con.execute("SELECT pg_advisory_xact_lock(27412026)")
        build = con.execute("SELECT * FROM meta.model_build WHERE id=true").fetchone()
        if (
            not build
            or build["signature"] != job["quality_signature"]
            or signature(con) != job["quality_signature"]
            or build["max_turn_minutes"] != cfg.max_turn_minutes
        ):
            raise ValueError("Warehouse changed after validation; rebuild and test")
        if dest.exists():
            manifest = read_json(dest / "publication.json")
            for name, item in manifest["files"].items():
                if sha256(dest / name) != item["sha256"]:
                    raise ValueError("Existing publication was modified")
        else:
            if stage.exists():
                shutil.rmtree(stage)
            stage.mkdir()
            files = {name + ".parquet": export_table(con, name, stage / (name + ".parquet")) for name in EXPORTS}
            manifest = {
                "publication_id": publication,
                "created_at": now(),
                "mode": cfg.mode,
                "signature": job["quality_signature"],
                "model_sha256": code_hash,
                "max_turn_minutes": cfg.max_turn_minutes,
                "versions": active_versions(con),
                "dbt_tests_passed": job["quality_test_count"],
                "files": files,
                "metric_definitions": "docs/METRICS.md",
                "interpretation": "Inferred intervals between adjacent reported legs, not observed ground-service durations.",
            }
            write_json(stage / "publication.json", manifest)
            os.replace(stage, dest)
        con.execute(
            "INSERT INTO meta.publications VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
                publication,
                job["run_id"],
                manifest["created_at"],
                job["quality_signature"],
                str(dest.relative_to(cfg.root)),
                Jsonb(manifest),
            ),
        )
    write_json(
        cfg.root / "gold/latest.json", {"publication_id": publication, "relative_path": str(dest.relative_to(cfg.root))}
    )
    return manifest

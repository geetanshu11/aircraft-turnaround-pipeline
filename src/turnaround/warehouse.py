"""PostgreSQL atomic month replacement, COPY, source revisions and checkpoints."""

from contextlib import contextmanager
import os
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
import pyarrow.parquet as pq
from .core import digest, now
from .processing import verify_manifest
from .schema import TYPES


def connection_kwargs(cfg):
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", f"aircraft_{cfg.mode}"),
        "user": os.getenv("PGUSER", "aircraft"),
        "password": os.getenv("PGPASSWORD", ""),
        "connect_timeout": 10,
        "options": "-c timezone=UTC -c statement_timeout=1800000",
        "application_name": "aircraft-platform",
    }


@contextmanager
def connect(cfg, autocommit=False):
    with psycopg.connect(**connection_kwargs(cfg), autocommit=autocommit, row_factory=dict_row) as con:
        yield con


def initialize(cfg):
    cfg.initialize()
    with connect(cfg) as con:
        con.execute("SELECT pg_advisory_xact_lock(27412026)")
        con.execute("""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS meta;
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE IF NOT EXISTS meta.project (id boolean PRIMARY KEY DEFAULT true CHECK(id),mode text NOT NULL);
        CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
          run_id text PRIMARY KEY,started_at timestamptz NOT NULL,ended_at timestamptz,
          status text NOT NULL,mode text NOT NULL,months jsonb NOT NULL,error text);
        CREATE TABLE IF NOT EXISTS meta.task_events (
          event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,run_id text NOT NULL,
          task text NOT NULL,status text NOT NULL,recorded_at timestamptz NOT NULL DEFAULT now(),detail jsonb NOT NULL);
        CREATE TABLE IF NOT EXISTS meta.partition_versions (
          version_id text PRIMARY KEY,month text NOT NULL,source_sha256 text NOT NULL,reference_hash text NOT NULL,
          raw_rows bigint NOT NULL,accepted_rows bigint NOT NULL,rejected_rows bigint NOT NULL,duplicate_rows bigint NOT NULL,
          received_at timestamptz NOT NULL,first_loaded_at timestamptz NOT NULL DEFAULT now(),manifest jsonb NOT NULL,
          CHECK(raw_rows=accepted_rows+rejected_rows+duplicate_rows));
        CREATE TABLE IF NOT EXISTS meta.checkpoints (
          month text PRIMARY KEY,version_id text NOT NULL REFERENCES meta.partition_versions,
          committed_at timestamptz NOT NULL DEFAULT now());
        CREATE TABLE IF NOT EXISTS meta.partition_activations (
          activation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,run_id text NOT NULL,
          month text NOT NULL,version_id text NOT NULL REFERENCES meta.partition_versions,previous_version_id text,
          committed_at timestamptz NOT NULL DEFAULT now(),inserted_rows bigint NOT NULL,
          updated_rows bigint NOT NULL,withdrawn_rows bigint NOT NULL);
        CREATE TABLE IF NOT EXISTS meta.flight_changes (
          activation_id bigint REFERENCES meta.partition_activations,flight_id text NOT NULL,change_type text NOT NULL,
          old_hash text,new_hash text,old_values jsonb,new_values jsonb,PRIMARY KEY(activation_id,flight_id));
        CREATE TABLE IF NOT EXISTS meta.quality_results (
          check_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,run_id text NOT NULL,stage text NOT NULL,
          recorded_at timestamptz NOT NULL DEFAULT now(),results jsonb NOT NULL);
        CREATE TABLE IF NOT EXISTS raw.sequence_barriers (
          month text NOT NULL,tail_number text NOT NULL,flight_date date NOT NULL,PRIMARY KEY(month,tail_number,flight_date));
        CREATE TABLE IF NOT EXISTS raw.blocked_months (month text PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS meta.model_build (
          id boolean PRIMARY KEY DEFAULT true CHECK(id),signature text NOT NULL,
          max_turn_minutes integer NOT NULL,built_at timestamptz NOT NULL DEFAULT now());
        CREATE TABLE IF NOT EXISTS meta.publications (
          publication_id text PRIMARY KEY,run_id text NOT NULL,created_at timestamptz NOT NULL,
          signature text NOT NULL,relative_path text NOT NULL,manifest jsonb NOT NULL);
        """)
        row = con.execute("SELECT mode FROM meta.project WHERE id=true").fetchone()
        if row and row["mode"] != cfg.mode:
            raise ValueError("Cannot mix demo, fixture or live in one database")
        con.execute("INSERT INTO meta.project VALUES(true,%s) ON CONFLICT DO NOTHING", (cfg.mode,))
        definitions = ",".join(f'"{k}" {v}' for k, v in TYPES.items())
        con.execute(f"""CREATE TABLE IF NOT EXISTS raw.flights ({definitions},
          version_id text NOT NULL REFERENCES meta.partition_versions,PRIMARY KEY(flight_id),
          CHECK(month=to_char(flight_date,'YYYY-MM')),CHECK(cancelled IN (0,1)),CHECK(diverted IN (0,1)),
          CHECK(origin_lat BETWEEN -90 AND 90),CHECK(dest_lat BETWEEN -90 AND 90),
          CHECK(origin_lon BETWEEN -180 AND 180),CHECK(dest_lon BETWEEN -180 AND 180))""")
        con.execute("CREATE INDEX IF NOT EXISTS flights_month_idx ON raw.flights(month)")
        con.execute(
            "CREATE INDEX IF NOT EXISTS flights_tail_time_idx ON raw.flights(tail_number,scheduled_dep_utc,flight_id)"
        )
        con.execute("CREATE INDEX IF NOT EXISTS events_run_idx ON meta.task_events(run_id)")


def record_event(cfg, run_id, task, status, detail):
    with connect(cfg) as con:
        con.execute(
            "INSERT INTO meta.task_events(run_id,task,status,detail) VALUES(%s,%s,%s,%s)",
            (run_id, task, status, Jsonb(detail)),
        )


def start_run(cfg, run_id, months):
    with connect(cfg) as con:
        con.execute(
            """INSERT INTO meta.pipeline_runs(run_id,started_at,status,mode,months)
          VALUES(%s,%s,'running',%s,%s) ON CONFLICT(run_id)
          DO UPDATE SET status='running',error=NULL,ended_at=NULL""",
            (run_id, now(), cfg.mode, Jsonb(months)),
        )


def finish_run(cfg, run_id, status, error=None):
    with connect(cfg) as con:
        con.execute(
            "UPDATE meta.pipeline_runs SET ended_at=now(),status=%s,error=%s WHERE run_id=%s", (status, error, run_id)
        )


def active_versions(con):
    return list(con.execute("SELECT month,version_id FROM meta.checkpoints ORDER BY month"))


def signature(con):
    return digest(active_versions(con))


def load_partition(cfg, manifest, run_id, fail_before_commit=False):
    verify_manifest(cfg, manifest)
    month, version = manifest["month"], manifest["version_id"]
    with connect(cfg) as con:
        con.execute("SELECT pg_advisory_xact_lock(27412026)")
        current = con.execute("SELECT version_id FROM meta.checkpoints WHERE month=%s", (month,)).fetchone()
        if current and current["version_id"] == version:
            count = con.execute("SELECT count(*) AS n FROM raw.flights WHERE month=%s", (month,)).fetchone()["n"]
            if count != manifest["accepted_rows"]:
                raise ValueError("Checkpoint row count mismatch; rebuild from raw")
            return {"month": month, "version_id": version, "rows": count, "status": "unchanged"}
        con.execute("CREATE TEMP TABLE inbound (LIKE raw.flights INCLUDING DEFAULTS) ON COMMIT DROP")
        columns = list(TYPES) + ["version_id"]
        parquet = pq.ParquetFile(cfg.root / manifest["silver_relative_path"])
        method = os.getenv("PG_LOAD_METHOD", "copy")
        if method == "copy":
            query = sql.SQL("COPY inbound ({}) FROM STDIN").format(sql.SQL(",").join(map(sql.Identifier, columns)))
            with con.cursor().copy(query) as copy:
                for batch in parquet.iter_batches(batch_size=10_000):
                    for row in batch.to_pylist():
                        copy.write_row([row[k] for k in TYPES] + [version])
        elif method == "insert":
            # Explicit compatibility mode for small tests. No automatic fallback from COPY.
            for batch in parquet.iter_batches(batch_size=200):
                rows = [[r[k] for k in TYPES] + [version] for r in batch.to_pylist()]
                one = sql.SQL("({})").format(sql.SQL(",").join(sql.Placeholder() for _ in columns))
                stmt = sql.SQL("INSERT INTO inbound ({}) VALUES {}").format(
                    sql.SQL(",").join(map(sql.Identifier, columns)), sql.SQL(",").join(one for _ in rows)
                )
                con.execute(stmt, [v for r in rows for v in r])
        else:
            raise ValueError("PG_LOAD_METHOD must be copy or insert")
        counts = con.execute(
            """SELECT
          (SELECT count(*) FROM inbound i LEFT JOIN raw.flights f USING(flight_id) WHERE f.flight_id IS NULL) AS inserted,
          (SELECT count(*) FROM inbound i JOIN raw.flights f USING(flight_id) WHERE i.record_hash<>f.record_hash) AS updated,
          (SELECT count(*) FROM raw.flights f LEFT JOIN inbound i USING(flight_id) WHERE f.month=%s AND i.flight_id IS NULL) AS withdrawn""",
            (month,),
        ).fetchone()
        con.execute(
            """INSERT INTO meta.partition_versions
          (version_id,month,source_sha256,reference_hash,raw_rows,accepted_rows,rejected_rows,duplicate_rows,received_at,manifest)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (
                version,
                month,
                manifest["sha256"],
                manifest["reference_hash"],
                manifest["raw_rows"],
                manifest["accepted_rows"],
                manifest["rejected_rows"],
                manifest["duplicate_rows"],
                manifest["received_at"],
                Jsonb(manifest),
            ),
        )
        aid = con.execute(
            """INSERT INTO meta.partition_activations
          (run_id,month,version_id,previous_version_id,inserted_rows,updated_rows,withdrawn_rows)
          VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING activation_id""",
            (
                run_id,
                month,
                version,
                current["version_id"] if current else None,
                counts["inserted"],
                counts["updated"],
                counts["withdrawn"],
            ),
        ).fetchone()["activation_id"]
        con.execute(
            """INSERT INTO meta.flight_changes
          SELECT %s,i.flight_id,'updated',f.record_hash,i.record_hash,to_jsonb(f),to_jsonb(i)
          FROM inbound i JOIN raw.flights f USING(flight_id) WHERE i.record_hash<>f.record_hash""",
            (aid,),
        )
        con.execute(
            """INSERT INTO meta.flight_changes SELECT %s,f.flight_id,'withdrawn',f.record_hash,NULL,to_jsonb(f),NULL
          FROM raw.flights f LEFT JOIN inbound i USING(flight_id) WHERE f.month=%s AND i.flight_id IS NULL""",
            (aid, month),
        )
        con.execute("DELETE FROM raw.flights WHERE month=%s", (month,))
        con.execute("INSERT INTO raw.flights SELECT * FROM inbound")
        con.execute("DELETE FROM raw.sequence_barriers WHERE month=%s", (month,))
        for tail, date in manifest["barriers"]:
            con.execute("INSERT INTO raw.sequence_barriers VALUES(%s,%s,%s)", (month, tail, date))
        con.execute("DELETE FROM raw.blocked_months WHERE month=%s", (month,))
        if manifest["block_month"]:
            con.execute("INSERT INTO raw.blocked_months VALUES(%s)", (month,))
        con.execute(
            """INSERT INTO meta.checkpoints VALUES(%s,%s,now()) ON CONFLICT(month)
          DO UPDATE SET version_id=EXCLUDED.version_id,committed_at=EXCLUDED.committed_at""",
            (month, version),
        )
        con.execute(
            "INSERT INTO meta.quality_results(run_id,stage,results) VALUES(%s,%s,%s)",
            (
                run_id,
                "load:" + month,
                Jsonb(
                    {
                        k: manifest[k]
                        for k in [
                            "raw_rows",
                            "accepted_rows",
                            "rejected_rows",
                            "duplicate_rows",
                            "time_status_counts",
                            "block_month",
                        ]
                    }
                ),
            ),
        )
        if fail_before_commit:
            raise RuntimeError("Injected failure before commit")
    return {"month": month, "version_id": version, "rows": manifest["accepted_rows"], "status": "loaded", **counts}


def health(cfg):
    with connect(cfg) as con:
        checkpoints = list(
            con.execute("""SELECT c.month,c.version_id,c.committed_at,v.accepted_rows,
          v.manifest->>'source_kind' AS source_kind FROM meta.checkpoints c
          JOIN meta.partition_versions v USING(version_id) ORDER BY c.month""")
        )
        gaps = list(
            con.execute("""WITH bounds AS (
          SELECT min(month)::date AS lo,max(month)::date AS hi FROM (SELECT month||'-01' AS month FROM meta.checkpoints) m)
          SELECT to_char(d,'YYYY-MM') AS missing_month FROM bounds,
          LATERAL generate_series(lo,hi,interval '1 month') d
          LEFT JOIN meta.checkpoints c ON c.month=to_char(d,'YYYY-MM') WHERE c.month IS NULL""")
        )
        runs = list(
            con.execute(
                "SELECT run_id,started_at,ended_at,status,error FROM meta.pipeline_runs ORDER BY started_at DESC LIMIT 20"
            )
        )
        return {
            "checkpoints": checkpoints,
            "maximum_loaded_month": checkpoints[-1]["month"] if checkpoints else None,
            "missing_months_between_bounds": gaps,
            "recent_runs": runs,
        }

"""Chunked normalization and global deduplication reused from legacy ingestion."""

import os
import shutil
import zipfile
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from .core import (
    TRANSFORM_VERSION,
    digest,
    implementation_hash,
    local_path,
    now,
    read_json,
    reference_snapshot,
    sha256,
    write_json,
)
from .schema import ARROW_SCHEMA
from .source import inspect_source
from .transform import normalize


def sql_path(path):
    return "'" + str(path).replace("'", "''") + "'"


def sequence_barriers(rejected, month):
    barriers = set()
    block_month = False
    for row in rejected.to_dict("records"):
        tail = row.get("Tail_Number", row.get("tail_number"))
        tail = "" if tail is None or pd.isna(tail) else str(tail).strip().upper()
        date = pd.to_datetime(row.get("FlightDate", row.get("flight_date")), errors="coerce")
        if tail and tail not in {"UNKNOWN", "UNKNOW"} and pd.notna(date) and date.strftime("%Y-%m") == month:
            barriers.add((tail, date.strftime("%Y-%m-%d")))
        else:
            block_month = True
    return barriers, block_month


def process_archive(cfg, raw, metadata):
    airports, refhash = reference_snapshot(cfg)
    implementation = implementation_hash()
    month = metadata["month"]
    version = digest(
        {
            "source": metadata["sha256"],
            "reference": refhash,
            "transform": TRANSFORM_VERSION,
            "implementation": implementation,
            "mode": cfg.mode,
            "month": month,
        }
    )
    destination = cfg.root / "silver" / f"month={month}" / f"version={version}"
    manifest_path = destination / "manifest.json"
    if manifest_path.exists():
        manifest = read_json(manifest_path)
        verify_manifest(cfg, manifest)
        if manifest["rejected_rows"] / manifest["raw_rows"] > cfg.reject_fraction:
            raise ValueError("Quality gate failed for cached snapshot")
        return manifest
    stage = cfg.root / "staging" / version
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir()
    quarantine = cfg.root / "quarantine" / version
    if quarantine.exists():
        shutil.rmtree(quarantine)
    quarantine.mkdir()
    schema = inspect_source(raw)
    raw_rows = rejected_rows = 0
    barriers = set()
    block_month = False
    with zipfile.ZipFile(raw) as z, z.open(schema["csv_member"]) as f:
        for index, chunk in enumerate(pd.read_csv(f, dtype=str, keep_default_na=False, chunksize=cfg.chunk_rows)):
            good, bad = normalize(chunk, month, airports, raw_rows + 1)
            raw_rows += len(chunk)
            rejected_rows += len(bad)
            pq.write_table(
                pa.Table.from_pandas(good, schema=ARROW_SCHEMA, preserve_index=False),
                stage / f"part-{index:05d}.parquet",
            )
            if len(bad):
                bad.to_parquet(quarantine / f"invalid-{index:05d}.parquet", index=False)
                b, unknown = sequence_barriers(bad, month)
                barriers |= b
                block_month |= unknown
    if not raw_rows:
        raise ValueError("Empty source month")
    con = duckdb.connect()
    con.execute("SET memory_limit='512MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory={sql_path(stage / 'spill')}")
    try:
        con.execute(
            f"CREATE VIEW staged AS SELECT * FROM read_parquet({sql_path(stage / 'part-*.parquet')},hive_partitioning=false)"
        )
        con.execute(
            "CREATE TEMP TABLE conflicts AS SELECT flight_id FROM staged GROUP BY flight_id HAVING count(DISTINCT record_hash)>1"
        )
        conflicting = con.execute("SELECT count(*) FROM staged SEMI JOIN conflicts USING(flight_id)").fetchone()[0]
        if conflicting:
            con.execute(
                f"COPY (SELECT s.*, 'conflicting_natural_key' AS reject_reason FROM staged s SEMI JOIN conflicts USING(flight_id)) TO {sql_path(quarantine / 'conflicts.parquet')} (FORMAT PARQUET)"
            )
            conflict_rows = (
                con.execute("SELECT s.tail_number,s.flight_date FROM staged s SEMI JOIN conflicts USING(flight_id)")
                .to_arrow_table()
                .to_pandas()
            )
            b, unknown = sequence_barriers(conflict_rows, month)
            barriers |= b
            block_month |= unknown
        con.execute("""CREATE TEMP TABLE accepted AS SELECT * FROM staged ANTI JOIN conflicts USING(flight_id)
                       QUALIFY row_number() OVER(PARTITION BY flight_id ORDER BY source_row)=1""")
        accepted = con.execute("SELECT count(*) FROM accepted").fetchone()[0]
        duplicates = raw_rows - rejected_rows - conflicting - accepted
        rejected_rows += conflicting
        quality = {
            "raw_rows": raw_rows,
            "accepted_rows": accepted,
            "rejected_rows": rejected_rows,
            "duplicate_rows": duplicates,
        }
        write_json(quarantine / "quality.json", {**quality, "at": now()})
        if rejected_rows / raw_rows > cfg.reject_fraction or accepted == 0:
            raise ValueError(f"Quality gate failed: {rejected_rows}/{raw_rows} rejected")
        temporary = stage / "flights.parquet"
        con.execute(
            f"COPY (SELECT * FROM accepted ORDER BY flight_id) TO {sql_path(temporary)} (FORMAT PARQUET,COMPRESSION ZSTD)"
        )
        statuses = dict(con.execute("SELECT time_status,count(*) FROM accepted GROUP BY time_status").fetchall())
        coverage = con.execute(
            "SELECT min(flight_date)::VARCHAR,max(flight_date)::VARCHAR,count(DISTINCT flight_date) FROM accepted"
        ).fetchone()
        manifest = {
            **{k: v for k, v in metadata.items() if k != "download_relative_path"},
            **schema,
            **quality,
            "version_id": version,
            "reference_hash": refhash,
            "transform_version": TRANSFORM_VERSION,
            "implementation_sha256": implementation,
            "processed_at": now(),
            "time_status_counts": statuses,
            "barriers": sorted(barriers),
            "block_month": block_month,
            "date_start": coverage[0],
            "date_end": coverage[1],
            "observed_days": coverage[2],
            "silver_relative_path": str((destination / "flights.parquet").relative_to(cfg.root)),
            "silver_sha256": sha256(temporary),
        }
        destination.mkdir(parents=True, exist_ok=True)
        os.replace(temporary, destination / "flights.parquet")
        write_json(manifest_path, manifest)
    finally:
        con.close()
    shutil.rmtree(stage)
    return manifest


def verify_manifest(cfg, manifest):
    for path_key, sha_key in [("raw_relative_path", "sha256"), ("silver_relative_path", "silver_sha256")]:
        if sha256(local_path(cfg, manifest[path_key])) != manifest[sha_key]:
            raise ValueError(f"Artifact checksum mismatch: {path_key}")
    if manifest["raw_rows"] != manifest["accepted_rows"] + manifest["rejected_rows"] + manifest["duplicate_rows"]:
        raise ValueError("Row reconciliation failure")

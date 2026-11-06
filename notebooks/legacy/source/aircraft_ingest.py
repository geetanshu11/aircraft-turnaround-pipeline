"""Monthly ingestion with immutable source files and atomic partition replacement."""
from __future__ import annotations
from pathlib import Path, PurePosixPath
from datetime import datetime, timezone
import json
import os
import shutil
import time
import zipfile
import requests
import pandas as pd

from aircraft_core import VERSION, Config, connect, digest, log, now, reference_snapshot, sha256, source_url, sql_path, write_json
from aircraft_transform import COLUMNS, normalize


def validate_zip(path):
    with zipfile.ZipFile(path) as z:
        names = z.namelist()
        if len(names) != len(set(names)):
            raise ValueError("Duplicate ZIP members")
        for info in z.infolist():
            name = PurePosixPath(info.filename)
            if name.is_absolute() or ".." in name.parts or "\\" in info.filename:
                raise ValueError("Unsafe ZIP path")
        files = [i for i in z.infolist() if i.filename.lower().endswith(".csv")]
        if len(files) != 1 or files[0].file_size > 4 * 1024**3:
            raise ValueError("I expected one CSV smaller than 4 GiB")
        return files[0].filename


def download_zip(url, target, session=None, sleeper=time.sleep, attempts=4):
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(".partial")
    session = session or requests.Session()
    for attempt in range(attempts):
        try:
            with session.get(url, stream=True, timeout=(20, 90),
                             headers={"User-Agent": "AircraftTurnaroundPortfolio/1.0"}) as response:
                if response.status_code in {429, 500, 502, 503, 504}:
                    retry_after = response.headers.get("Retry-After", "")
                    delay = float(retry_after) if retry_after.isdigit() else 2**attempt
                    if delay > 60 or attempt == attempts - 1:
                        response.raise_for_status()
                    sleeper(delay)
                    continue
                response.raise_for_status()
                count = 0
                with open(partial, "wb") as f:
                    for chunk in response.iter_content(1024 * 1024):
                        count += len(chunk)
                        if count > 200 * 1024**2:
                            raise ValueError("Download exceeded the 200 MiB safety limit")
                        f.write(chunk)
                validate_zip(partial)
                os.replace(partial, target)
                return {"url": url, "received_at": now(), "sha256": sha256(target),
                        "bytes": count, "etag": response.headers.get("ETag"),
                        "last_modified": response.headers.get("Last-Modified")}
        except (requests.Timeout, requests.ConnectionError):
            if attempt == attempts - 1:
                raise
            sleeper(2**attempt)
        finally:
            partial.unlink(missing_ok=True)
    raise RuntimeError("Download retry budget exhausted")


def acquire(cfg, month, local_zip=None, source_metadata=None):
    pointer = cfg.root / "bronze" / month / "latest.json"
    if local_zip is None and pointer.exists() and not cfg.refresh:
        meta = json.loads(pointer.read_text())
        path = cfg.root / meta["raw_relative_path"]
        if sha256(path) != meta["sha256"]:
            raise ValueError("Cached source checksum mismatch")
        return path, meta
    temp = cfg.root / "staging" / f"{month}.download.zip"
    if local_zip is not None:
        validate_zip(local_zip)
        metadata = dict(source_metadata or {})
        metadata.update({"sha256": sha256(local_zip), "bytes": Path(local_zip).stat().st_size,
                         "received_at": metadata.get("received_at", now()),
                         "url": metadata.get("url", "local-import")})
        if source_metadata and source_metadata.get("sha256") != metadata["sha256"]:
            raise ValueError("Imported source checksum mismatch")
        shutil.copyfile(local_zip, temp)
    else:
        metadata = download_zip(source_url(month), temp)
    raw = cfg.root / "bronze" / month / f"{metadata['sha256']}.zip"
    raw.parent.mkdir(parents=True, exist_ok=True)
    os.replace(temp, raw)
    metadata.update({"month": month, "mode": cfg.mode, "raw_relative_path": str(raw.relative_to(cfg.root)),
                     "source_kind": metadata.get("source_kind", "BTS_full_month")})
    write_json(raw.with_suffix(".json"), metadata)
    write_json(pointer, metadata)
    return raw, metadata


def event(con, run, month, task, status, detail):
    con.execute("INSERT INTO task_events VALUES (?,?,?,?,?,?)", [run, task, month, status, now(), json.dumps(detail, default=str)])


def ingest_month(cfg, month, run, local_zip=None, source_metadata=None, fail_before_commit=False):
    con = connect(cfg)
    try:
        raw, meta = acquire(cfg, month, local_zip, source_metadata)
        event(con, run, month, "acquire", "succeeded", {"sha256": meta["sha256"], "bytes": meta["bytes"]})
        airports, refhash = reference_snapshot(cfg)
        version = digest({"source": meta["sha256"], "reference": refhash, "transform": VERSION,
                          "month": month, "mode": cfg.mode})
        current = con.execute("SELECT version_id FROM checkpoints WHERE partition_month=?", [month]).fetchone()
        if current and current[0] == version:
            expected = con.execute("SELECT accepted_rows FROM partition_versions WHERE version_id=?", [version]).fetchone()[0]
            count = con.execute("SELECT count(*) FROM flights WHERE month=?", [month]).fetchone()[0]
            if count != expected:
                raise ValueError("Checkpoint and warehouse row count disagree; rebuild from raw data")
            result = {"month": month, "status": "unchanged", "rows": count, "version_id": version}
            event(con, run, month, "load", "unchanged", result)
            return result
        stage = cfg.root / "staging" / version
        if stage.exists():
            shutil.rmtree(stage)
        stage.mkdir()
        quarantine = cfg.root / "quarantine" / version
        quarantine.mkdir(exist_ok=True)
        member = validate_zip(raw)
        raw_rows = rejected_rows = 0
        with zipfile.ZipFile(raw) as z, z.open(member) as f:
            for index, chunk in enumerate(pd.read_csv(f, dtype=str, keep_default_na=False, chunksize=cfg.chunk_rows)):
                good, bad = normalize(chunk, month, airports, raw_rows + 1)
                raw_rows += len(chunk)
                rejected_rows += len(bad)
                good.to_parquet(stage / f"part-{index:05d}.parquet", index=False)
                if len(bad):
                    bad.to_parquet(quarantine / f"invalid-{index:05d}.parquet", index=False)
        if raw_rows == 0:
            raise ValueError("Empty source month")
        con.execute(f"CREATE TEMP TABLE staged AS SELECT * FROM read_parquet({sql_path(stage / '*.parquet')}, union_by_name=true)")
        con.execute("CREATE TEMP TABLE conflicts AS SELECT flight_id FROM staged GROUP BY flight_id HAVING count(DISTINCT record_hash)>1")
        conflicts = con.execute("SELECT count(*) FROM staged SEMI JOIN conflicts USING(flight_id)").fetchone()[0]
        if conflicts:
            con.execute(f"COPY (SELECT s.*, 'conflicting_natural_key' AS reject_reason FROM staged s SEMI JOIN conflicts USING(flight_id)) TO {sql_path(quarantine / 'conflicts.parquet')} (FORMAT PARQUET)")
        con.execute("""CREATE TEMP TABLE accepted AS
                     SELECT * FROM staged ANTI JOIN conflicts USING(flight_id)
                     QUALIFY row_number() OVER(PARTITION BY flight_id ORDER BY source_row)=1""")
        accepted = con.execute("SELECT count(*) FROM accepted").fetchone()[0]
        duplicates = raw_rows - rejected_rows - conflicts - accepted
        rejected_rows += conflicts
        if rejected_rows / raw_rows > .01 or accepted == 0:
            raise ValueError(f"Quality gate failed: {rejected_rows}/{raw_rows} rows rejected")
        silver = cfg.root / "silver" / f"month={month}" / f"{version}.parquet"
        silver.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY accepted TO {sql_path(silver.with_suffix('.partial'))} (FORMAT PARQUET, COMPRESSION ZSTD)")
        os.replace(silver.with_suffix(".partial"), silver)
        con.execute("CREATE TABLE IF NOT EXISTS flights AS SELECT *, ''::VARCHAR AS version_id FROM accepted WHERE false")
        inserted = con.execute("SELECT count(*) FROM accepted a ANTI JOIN flights f USING(flight_id)").fetchone()[0]
        updated = con.execute("SELECT count(*) FROM accepted a JOIN flights f USING(flight_id) WHERE a.record_hash<>f.record_hash").fetchone()[0]
        withdrawn = con.execute("SELECT count(*) FROM flights f ANTI JOIN accepted a USING(flight_id) WHERE f.month=?", [month]).fetchone()[0]
        values = "struct_pack(tail_number := {a}.tail_number, dep_delay_min := {a}.dep_delay_min, arr_delay_min := {a}.arr_delay_min, cancelled := {a}.cancelled, diverted := {a}.diverted)"
        con.execute("BEGIN")
        try:
            con.execute(f"""INSERT INTO flight_changes
               SELECT ?, a.flight_id, 'updated', f.record_hash, a.record_hash,
               to_json({values.format(a='f')}), to_json({values.format(a='a')}), ?
               FROM accepted a JOIN flights f USING(flight_id) WHERE a.record_hash<>f.record_hash""", [version, run])
            con.execute(f"""INSERT INTO flight_changes
               SELECT ?, f.flight_id, 'withdrawn', f.record_hash, NULL, to_json({values.format(a='f')}), NULL, ?
               FROM flights f ANTI JOIN accepted a USING(flight_id) WHERE f.month=?""", [version, run, month])
            con.execute("DELETE FROM flights WHERE month=?", [month])
            con.execute("INSERT INTO flights SELECT *, ? AS version_id FROM accepted", [version])
            con.execute("INSERT OR IGNORE INTO partition_versions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        [version, month, meta["sha256"], refhash, VERSION, meta["received_at"], now(),
                         str(raw.relative_to(cfg.root)), str(silver.relative_to(cfg.root)), raw_rows, accepted,
                         rejected_rows, duplicates, inserted, updated, withdrawn, json.dumps(meta)])
            con.execute("INSERT OR REPLACE INTO checkpoints VALUES (?,?,?)", [month, version, now()])
            con.execute("INSERT INTO partition_activations VALUES (?,?,?,?,?,?,?,?)",
                        [run,month,version,current[0] if current else None,now(),inserted,updated,withdrawn])
            for name, n, severity, detail in [
                ("rejected_records", rejected_rows, "warning" if rejected_rows else "pass", "See quarantine files"),
                ("exact_duplicates", duplicates, "warning" if duplicates else "pass", "Identical business records collapsed"),
                ("row_reconciliation", raw_rows - accepted - rejected_rows - duplicates, "pass", "raw = accepted + rejected + duplicates")]:
                con.execute("INSERT INTO quality_checks VALUES (?,?,?,?,?,?,?)", [run, month, name, severity, n, detail, now()])
            for status, n in con.execute("SELECT time_status,count(*) FROM accepted GROUP BY time_status").fetchall():
                con.execute("INSERT INTO quality_checks VALUES (?,?,?,?,?,?,?)", [run, month, "time:"+status,
                            "pass" if status in {"valid", "cancelled", "diverted"} else "warning", n,
                            "Invalid clocks remain in reliability counts but cannot form turnarounds", now()])
            if fail_before_commit:
                raise RuntimeError("Injected failure before commit")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        result = {"month": month, "status": "loaded", "rows": accepted, "raw_rows": raw_rows,
                  "rejected": rejected_rows, "duplicates": duplicates, "inserted": inserted,
                  "updated": updated, "withdrawn": withdrawn, "version_id": version}
        event(con, run, month, "load", "succeeded", result)
        log(cfg, "partition_committed", **result)
        shutil.rmtree(stage)
        return result
    except Exception as exc:
        event(con, run, month, "ingest", "failed", {"error": str(exc)[:1000]})
        raise
    finally:
        con.close()

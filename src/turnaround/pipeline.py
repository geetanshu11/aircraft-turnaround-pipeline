"""Shared CLI/Airflow stages. Only job paths cross Airflow task boundaries."""

from pathlib import Path
import json
import os
import shutil
import subprocess
import sys
import uuid
from .core import (
    Config,
    ROOT,
    digest,
    local_path,
    log,
    model_hash,
    now,
    read_json,
    sha256,
    source_url,
    validate_month,
    write_json,
    writer_lock,
)
from .processing import process_archive
from .source import download_zip, inspect_source
from .storage import put_immutable, mirror_s3
from . import warehouse

STAGES = (
    "download",
    "validate",
    "raw_storage",
    "parquet_processing",
    "warehouse_loading",
    "dbt_transformation",
    "quality_tests",
    "analytical_publishing",
)


def create_job(cfg, months, run_id=None, inputs=None):
    months = sorted(set(map(validate_month, months)))
    if not months:
        raise ValueError("At least one month is required")
    if cfg.mode == "demo" and months != ["2025-03"]:
        raise ValueError("The demo contains March 8–10, 2025 only")
    rid = run_id or uuid.uuid4().hex
    path = cfg.root / "jobs" / digest(rid)[:24] / "job.json"
    config = {
        "root": str(cfg.root),
        "mode": cfg.mode,
        "chunk_rows": cfg.chunk_rows,
        "max_turn_minutes": cfg.max_turn_minutes,
        "reject_fraction": cfg.reject_fraction,
        "refresh": cfg.refresh,
    }
    with writer_lock(cfg):
        lease = cfg.root / "active_job.json"
        if lease.exists() and read_json(lease)["run_id"] != rid:
            raise RuntimeError("Another job owns this root. Resume it or abandon after verifying it has stopped.")
        if path.exists():
            job = read_json(path)
            if job["months"] != months or job["config"] != config or digest(job["inputs"]) != digest(inputs or {}):
                raise ValueError("Run ID cannot be reused with different inputs or configuration")
            return path
        warehouse.initialize(cfg)
        job = {
            "run_id": rid,
            "created_at": now(),
            "updated_at": now(),
            "status": "running",
            "months": months,
            "config": config,
            "inputs": inputs or {},
            "items": {},
            "completed": [],
        }
        write_json(path, job)
        write_json(lease, {"run_id": rid, "job": str(path)})
        warehouse.start_run(cfg, rid, months)
    return path


def run_dbt(cfg, path, command):
    binary = Path(sys.executable).with_name("dbt" + (".exe" if os.name == "nt" else ""))
    if not binary.exists():
        raise RuntimeError("dbt is missing from the application environment")
    env = os.environ.copy()
    env.update(
        PGDATABASE=warehouse.connection_kwargs(cfg)["dbname"],
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
        DBT_LOG_PATH=str(path.parent / "dbt-logs"),
    )
    result = subprocess.run(
        [
            str(binary),
            command,
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            str(ROOT / "dbt"),
            "--target-path",
            str(path.parent / "dbt-target"),
            "--threads",
            os.getenv("DBT_THREADS", "2"),
            "--vars",
            json.dumps({"max_turn_minutes": cfg.max_turn_minutes}),
        ],
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    output = path.parent / f"dbt-{command}.log"
    output.write_text(result.stdout, encoding="utf-8")
    print(result.stdout, flush=True)
    if result.returncode:
        raise RuntimeError(f"dbt {command} failed (exit {result.returncode}); inspect {output}")
    if command in {"run", "test"}:
        shutil.copyfile(path.parent / "dbt-target/run_results.json", path.parent / f"dbt-{command}-results.json")


def _download(cfg, job):
    for month in job["months"]:
        target = cfg.root / "jobs" / digest(job["run_id"])[:24] / f"{month}.zip"
        if month in job["items"] and target.exists() and sha256(target) == job["items"][month]["sha256"]:
            continue
        explicit = job["inputs"].get(month)
        if cfg.mode == "demo" and explicit is None:
            explicit = {"path": str(ROOT / "demo/bts_demo.zip"), "metadata": read_json(ROOT / "demo/source.json")}
        pointer = cfg.root / "raw" / month / "latest.json"
        if explicit:
            src = Path(explicit["path"])
            meta = dict(explicit.get("metadata", {}))
            if meta.get("sha256") and sha256(src) != meta["sha256"]:
                raise ValueError("Imported source checksum mismatch")
            shutil.copyfile(src, target)
            meta = {
                **meta,
                "sha256": sha256(target),
                "bytes": target.stat().st_size,
                "received_at": meta.get("received_at", now()),
                "url": meta.get("url", "local-fixture"),
                "source_kind": meta.get(
                    "source_kind", "synthetic_fixture" if cfg.mode == "fixture" else "local_import"
                ),
            }
        elif pointer.exists() and not cfg.refresh:
            meta = read_json(pointer)
            src = local_path(cfg, meta["raw_relative_path"])
            if sha256(src) != meta["sha256"]:
                raise ValueError("Cached raw checksum mismatch")
            shutil.copyfile(src, target)
        else:
            meta = {**download_zip(source_url(month), target), "source_kind": "BTS_full_month"}
        job["items"][month] = {
            **meta,
            "month": month,
            "mode": cfg.mode,
            "download_relative_path": str(target.relative_to(cfg.root)),
        }


def _validate(cfg, job):
    for month, item in job["items"].items():
        path = local_path(cfg, item["download_relative_path"])
        if sha256(path) != item["sha256"]:
            raise ValueError("Downloaded artifact was modified")
        item.update(inspect_source(path))
        pointer = cfg.root / "raw" / month / "latest.json"
        old = read_json(pointer) if pointer.exists() else {}
        item["schema_changed"] = bool(old.get("schema_sha256") and old["schema_sha256"] != item["schema_sha256"])
        item["previous_schema_sha256"] = old.get("schema_sha256")


def _raw_storage(cfg, job):
    for month, item in job["items"].items():
        dest = cfg.root / "raw" / month / f"{item['sha256']}.zip"
        put_immutable(local_path(cfg, item["download_relative_path"]), dest)
        item["raw_relative_path"] = str(dest.relative_to(cfg.root))
        meta_path = dest.with_suffix(".json")
        if not meta_path.exists():
            write_json(meta_path, {k: v for k, v in item.items() if k != "download_relative_path"})
        else:
            item["received_at"] = read_json(meta_path)["received_at"]
        mirror_s3(dest, f"{cfg.mode}/{item['raw_relative_path']}")
        mirror_s3(meta_path, f"{cfg.mode}/{meta_path.relative_to(cfg.root)}")
        write_json(dest.parent / "latest.json", {k: v for k, v in item.items() if k != "download_relative_path"})


def _parquet_processing(cfg, job):
    for month, item in job["items"].items():
        job.setdefault("processed", {})[month] = process_archive(cfg, local_path(cfg, item["raw_relative_path"]), item)


def _warehouse_loading(cfg, job):
    job["loaded"] = []
    for month in job["months"]:
        result = warehouse.load_partition(cfg, job["processed"][month], job["run_id"])
        job["loaded"].append(result)
        log(cfg, "partition_committed", run_id=job["run_id"], **result)


def _dbt_transformation(cfg, job, path):
    with warehouse.connect(cfg) as con:
        before = warehouse.signature(con)
    code_hash = model_hash()
    run_dbt(cfg, path, "run")
    if model_hash() != code_hash:
        raise RuntimeError("dbt source files changed during execution")
    with warehouse.connect(cfg) as con:
        if warehouse.signature(con) != before:
            raise RuntimeError("Checkpoints changed during dbt; rebuild")
        con.execute(
            """INSERT INTO meta.model_build VALUES(true,%s,%s,now()) ON CONFLICT(id)
          DO UPDATE SET signature=EXCLUDED.signature,max_turn_minutes=EXCLUDED.max_turn_minutes,built_at=now()""",
            (before, cfg.max_turn_minutes),
        )
    job.update(model_signature=before, model_hash=code_hash)


def _quality_tests(cfg, job, path):
    with warehouse.connect(cfg) as con:
        if warehouse.signature(con) != job["model_signature"]:
            raise RuntimeError("Models are stale")
    if model_hash() != job["model_hash"]:
        raise RuntimeError("dbt definitions changed; rerun transformations")
    run_dbt(cfg, path, "test")
    result = read_json(path.parent / "dbt-test-results.json")
    if not result["results"] or any(r["status"] != "pass" for r in result["results"]):
        raise ValueError("All dbt tests must pass before publication")
    job.update(quality_signature=job["model_signature"], quality_test_count=len(result["results"]))
    with warehouse.connect(cfg) as con:
        con.execute(
            "INSERT INTO meta.quality_results(run_id,stage,results) VALUES(%s,%s,%s)",
            (
                job["run_id"],
                "dbt_tests",
                warehouse.Jsonb({"passed": len(result["results"]), "signature": job["quality_signature"]}),
            ),
        )


def execute_stage(job_path, stage):
    if stage not in STAGES:
        raise ValueError("Unknown stage")
    path = Path(job_path).resolve()
    job = read_json(path)
    cfg = Config(**job["config"])
    if not path.is_relative_to(cfg.root / "jobs"):
        raise ValueError("Job must be under its configured root")
    with writer_lock(cfg):
        lease = cfg.root / "active_job.json"
        if lease.exists() and read_json(lease)["run_id"] != job["run_id"]:
            raise RuntimeError("Another pipeline owns this root")
        if job["status"] == "abandoned":
            raise RuntimeError("Abandoned jobs cannot resume")
        index = STAGES.index(stage)
        if index and STAGES[index - 1] not in job["completed"]:
            raise RuntimeError("Preceding stage has not completed")
        write_json(lease, {"run_id": job["run_id"], "job": str(path)})
        job["completed"] = [s for s in job["completed"] if STAGES.index(s) < index]
        job.update(status="running", updated_at=now())
        write_json(path, job)
        warehouse.start_run(cfg, job["run_id"], job["months"])
        warehouse.record_event(cfg, job["run_id"], stage, "started", {})
        try:
            if index < 5:
                globals()["_" + stage](cfg, job)
            elif stage == "dbt_transformation":
                _dbt_transformation(cfg, job, path)
            elif stage == "quality_tests":
                _quality_tests(cfg, job, path)
            else:
                from .publishing import publish

                job["publication"] = publish(cfg, job)
            job["completed"].append(stage)
            job.update(error=None, updated_at=now())
            warehouse.record_event(cfg, job["run_id"], stage, "succeeded", {"months": job["months"]})
            if index == len(STAGES) - 1:
                job["status"] = "succeeded"
                warehouse.finish_run(cfg, job["run_id"], "succeeded")
            write_json(path, job)
            if index == len(STAGES) - 1:
                lease.unlink(missing_ok=True)
            log(cfg, "stage_succeeded", run_id=job["run_id"], stage=stage)
        except Exception as exc:
            job.update(
                status="failed",
                updated_at=now(),
                error={"stage": stage, "type": type(exc).__name__, "message": str(exc)[:2000], "at": now()},
            )
            write_json(path, job)
            try:
                warehouse.record_event(cfg, job["run_id"], stage, "failed", job["error"])
                warehouse.finish_run(cfg, job["run_id"], "failed", str(exc)[:2000])
            except Exception as audit_error:
                log(cfg, "audit_write_failed", run_id=job["run_id"], error=str(audit_error)[:1000])
            log(cfg, "stage_failed", run_id=job["run_id"], **job["error"])
            raise
    return str(path)


def resume(job_path):
    job = read_json(job_path)
    if job["status"] == "abandoned":
        raise RuntimeError("Abandoned jobs cannot resume")
    for stage in STAGES:
        if stage not in job["completed"]:
            execute_stage(job_path, stage)
    result = read_json(job_path)
    if result["status"] == "succeeded":
        cfg = Config(**result["config"])
        with writer_lock(cfg):
            lease = cfg.root / "active_job.json"
            if lease.exists() and read_json(lease)["run_id"] == result["run_id"]:
                lease.unlink()
    return result


def run_pipeline(cfg, months, inputs=None, run_id=None):
    return resume(create_job(cfg, months, run_id, inputs))


def abandon(cfg):
    with writer_lock(cfg):
        lease = cfg.root / "active_job.json"
        if not lease.exists():
            return
        path = local_path(cfg, read_json(lease)["job"])
        job = read_json(path)
        job.update(status="abandoned", updated_at=now())
        write_json(path, job)
        warehouse.finish_run(cfg, job["run_id"], "abandoned", "Explicitly abandoned by operator")
        lease.unlink()

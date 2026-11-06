"""Shared configuration, provenance, and small filesystem helpers."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone
from contextlib import contextmanager
import hashlib
import importlib.metadata
import json
import os
import re
import uuid

import airportsdata
import duckdb
import pandas as pd
from filelock import FileLock

VERSION = "1.0.0"
BASE = Path(__file__).parent
BTS_BASE = "https://transtats.bts.gov/PREZIP/"


@dataclass(frozen=True)
class Config:
    root: Path
    months: tuple[str, ...] = ("2025-01", "2025-02", "2025-03")
    mode: str = "live"
    refresh: bool = False
    chunk_rows: int = 100_000
    max_turn_minutes: int = 720
    min_airport_flights: int = 100

    def __post_init__(self):
        object.__setattr__(self, "root", Path(self.root).resolve())
        if self.mode not in {"live", "demo"}:
            raise ValueError("mode must be live or demo")
        if not self.months or len(set(self.months)) != len(self.months):
            raise ValueError("I need a non-empty, unique list of months")
        for month in self.months:
            if not re.fullmatch(r"20\d{2}-(0[1-9]|1[0-2])", month):
                raise ValueError(f"Invalid month: {month}")
        if self.chunk_rows < 1 or not 30 <= self.max_turn_minutes <= 2880:
            raise ValueError("Invalid chunk size or turnaround window")

    @property
    def db(self):
        return self.root / "warehouse.duckdb"


def now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".partial")
    temp.write_text(json.dumps(value, indent=2, default=str, allow_nan=False), encoding="utf-8")
    os.replace(temp, path)


def connect(cfg):
    con = duckdb.connect(str(cfg.db))
    con.execute("SET TimeZone='UTC'")
    con.execute("SET memory_limit='3GB'")
    con.execute("SET threads=2")
    con.execute("SET enable_progress_bar=false")
    return con


def initialize(cfg):
    for name in ["bronze", "silver", "quarantine", "gold", "logs", "reference", "staging"]:
        (cfg.root / name).mkdir(parents=True, exist_ok=True)
    meta_path = cfg.root / "project.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        if meta["mode"] != cfg.mode:
            raise ValueError("Live and demo data need separate runtime directories")
    else:
        write_json(meta_path, {"mode": cfg.mode, "created_at": now(), "project": "aircraft-turnaround"})
    con = connect(cfg)
    con.execute((BASE / "sql/schema.sql").read_text())
    con.close()


@contextmanager
def locked(cfg):
    cfg.root.mkdir(parents=True, exist_ok=True)
    with FileLock(str(cfg.root / "writer.lock"), timeout=0):
        initialize(cfg)
        yield


def log(cfg, event, **values):
    # I keep a readable log alongside the database audit tables.
    with open(cfg.root / "logs/pipeline.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps({"time": now(), "event": event, **values}, default=str) + "\n")


def run_id():
    return uuid.uuid4().hex


def reference_snapshot(cfg):
    # This is a current airport reference, not a historical airport master.
    airports = airportsdata.load("IATA")
    rows = [{"iata": k, "name": a["name"], "city": a["city"], "country": a["country"],
             "lat": a["lat"], "lon": a["lon"], "tz": a["tz"]} for k, a in sorted(airports.items())]
    meta = {"airportsdata_version": importlib.metadata.version("airportsdata"),
            "tzdata_version": importlib.metadata.version("tzdata"), "airports": rows}
    reference_hash = digest(meta)
    path = cfg.root / "reference" / f"airports_{reference_hash}.json"
    if not path.exists():
        write_json(path, meta)
    return pd.DataFrame(rows), reference_hash


def source_url(month):
    year, mm = month.split("-")
    return BTS_BASE + f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{int(mm)}.zip"


def sql_path(path):
    return "'" + str(path).replace("'", "''") + "'"

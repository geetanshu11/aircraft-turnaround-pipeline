"""Configuration, provenance and atomic files; adapted from legacy aircraft_core.py."""

from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import importlib.metadata
import json
import os
import re
import uuid
import airportsdata
import pandas as pd
from filelock import FileLock

ROOT = Path(__file__).resolve().parents[2]
TRANSFORM_VERSION = "2.0.0"
BTS_BASE = "https://transtats.bts.gov/PREZIP/"


def now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".partial")
    try:
        with temp.open("w", encoding="utf-8") as f:
            json.dump(value, f, indent=2, default=str, allow_nan=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def validate_month(month):
    if not re.fullmatch(r"20\d{2}-(0[1-9]|1[0-2])", month):
        raise ValueError(f"Invalid month: {month}")
    return month


def month_range(start, end):
    validate_month(start)
    validate_month(end)
    if start > end:
        raise ValueError("start must precede end")
    return tuple(str(p) for p in pd.period_range(start, end, freq="M"))


@dataclass(frozen=True)
class Config:
    root: Path
    mode: str = "demo"
    chunk_rows: int = 50_000
    max_turn_minutes: int = 720
    reject_fraction: float = 0.01
    refresh: bool = False

    def __post_init__(self):
        object.__setattr__(self, "root", Path(self.root).resolve())
        if self.mode not in {"demo", "live", "fixture"}:
            raise ValueError("Invalid data mode")
        if self.chunk_rows < 1 or not 30 <= self.max_turn_minutes <= 2880:
            raise ValueError("Invalid chunk size or turnaround window")
        if not 0 <= self.reject_fraction <= 1:
            raise ValueError("Invalid rejection threshold")

    @classmethod
    def from_env(cls, **overrides):
        mode = overrides.pop("mode", os.getenv("DATA_MODE", "demo"))
        root = overrides.pop("root", Path(os.getenv("DATA_ROOT", str(ROOT / "runtime"))) / mode)
        return cls(
            root=root,
            mode=mode,
            chunk_rows=int(os.getenv("CHUNK_ROWS", "50000")),
            max_turn_minutes=int(os.getenv("MAX_TURN_MINUTES", "720")),
            **overrides,
        )

    def initialize(self):
        self.root.mkdir(parents=True, exist_ok=True)
        with FileLock(str(self.root / "initialize.lock"), timeout=30):
            marker = self.root / "project.json"
            if marker.exists():
                if read_json(marker)["mode"] != self.mode:
                    raise ValueError("Demo, fixture and live require separate roots and databases")
            else:
                write_json(marker, {"mode": self.mode, "created_at": now()})
            for name in ["raw", "silver", "quarantine", "staging", "jobs", "reference", "gold", "logs"]:
                (self.root / name).mkdir(exist_ok=True)


def local_path(cfg, relative):
    path = (cfg.root / relative).resolve()
    if not path.is_relative_to(cfg.root):
        raise ValueError("Artifact path escapes data root")
    return path


def source_url(month):
    year, mm = validate_month(month).split("-")
    return BTS_BASE + f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{int(mm)}.zip"


def reference_snapshot(cfg):
    rows = [
        {
            "iata": k,
            "name": a["name"],
            "city": a["city"],
            "country": a["country"],
            "lat": a["lat"],
            "lon": a["lon"],
            "tz": a["tz"],
        }
        for k, a in sorted(airportsdata.load("IATA").items())
    ]
    meta = {
        "airportsdata_version": importlib.metadata.version("airportsdata"),
        "tzdata_version": importlib.metadata.version("tzdata"),
        "airports": rows,
    }
    refhash = digest(meta)
    path = cfg.root / "reference" / f"{refhash}.json"
    if path.exists() and read_json(path) != meta:
        raise ValueError("Reference snapshot was modified")
    if not path.exists():
        write_json(path, meta)
    return pd.DataFrame(rows), refhash


def implementation_hash():
    base = Path(__file__).parent
    return digest(
        {
            "files": {p: sha256(base / p) for p in ["transform.py", "processing.py", "schema.py"]},
            "packages": {p: importlib.metadata.version(p) for p in ["pandas", "numpy", "pyarrow", "duckdb"]},
        }
    )


def log(cfg, event, **values):
    entry = {"at": now(), "event": event, **values}
    with (cfg.root / "logs/pipeline.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, default=str) + "\n")
    print(json.dumps(entry, default=str), flush=True)


@contextmanager
def writer_lock(cfg):
    cfg.initialize()
    with FileLock(str(cfg.root / "writer.lock"), timeout=0):
        yield


def model_hash():
    return digest(
        {
            str(p.relative_to(ROOT / "dbt")): sha256(p)
            for p in sorted((ROOT / "dbt").rglob("*"))
            if p.is_file()
            and p.suffix in {".sql", ".yml"}
            and not {"target", "logs", "dbt_packages"}.intersection(p.parts)
        }
    )

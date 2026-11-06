from dataclasses import replace
import hashlib
import zipfile
import pandas as pd
import pytest
import requests
from turnaround.core import Config, now, sha256, write_json, read_json, month_range
from turnaround.processing import process_archive, sequence_barriers
from turnaround.source import inspect_source, validate_zip, download_zip, IncompleteDownload
from turnaround.storage import put_immutable, mirror_s3
from turnaround.transform import normalize
from .helpers import row, second, zip_rows


def prepare(cfg, tmp_path, rows, month="2025-03", name="source.zip"):
    source = zip_rows(tmp_path, rows, name)
    checksum = sha256(source)
    raw = cfg.root / "raw" / month / f"{checksum}.zip"
    put_immutable(source, raw)
    return process_archive(
        cfg,
        raw,
        {
            "month": month,
            "mode": cfg.mode,
            "sha256": checksum,
            "received_at": now(),
            "raw_relative_path": str(raw.relative_to(cfg.root)),
            "source_kind": "synthetic_fixture",
            "url": "fixture",
        },
    )


def test_legacy_business_key_preserved(airports):
    d, _ = normalize(pd.DataFrame([row()]), "2025-03", airports)
    assert d.flight_id.iloc[0] == hashlib.sha256(b"2025-03-10|19805|101|10721|12478|800").hexdigest()


def test_coordinates_rejected(airports):
    airports.loc[airports.iata.eq("BOS"), "lat"] = 99
    with pytest.raises(ValueError, match="coordinates"):
        normalize(pd.DataFrame([row()]), "2025-03", airports)


def test_duration_overflow_quarantined(airports):
    d, b = normalize(pd.DataFrame([row(DepDelay="1e200")]), "2025-03", airports)
    assert d.empty and len(b) == 1


def test_dedup_across_chunks(cfg, tmp_path):
    m = prepare(cfg, tmp_path, [row(), second(), row()])
    assert (m["accepted_rows"], m["duplicate_rows"], m["raw_rows"]) == (2, 1, 3)


def test_conflict_quarantine(cfg, tmp_path):
    with pytest.raises(ValueError, match="Quality gate"):
        prepare(cfg, tmp_path, [row(), row(Tail_Number="N999AA")])
    assert list((cfg.root / "quarantine").rglob("conflicts.parquet")) and not list(
        (cfg.root / "silver").rglob("manifest.json")
    )


def test_quarantine_barriers(cfg, tmp_path):
    m = prepare(
        replace(cfg, reject_fraction=0.5),
        tmp_path,
        [row(), second(), row(Flight_Number_Reporting_Airline="999", Distance="-1")],
    )
    assert m["barriers"] == [("N123AA", "2025-03-10")] and m["rejected_rows"] == 1 and not m["block_month"]


def test_unknown_assignment_blocks_month(cfg, tmp_path):
    assert prepare(replace(cfg, reject_fraction=0.5), tmp_path, [row(), second(), row(Tail_Number="", Distance="-1")])[
        "block_month"
    ]


@pytest.mark.parametrize(
    "tail,date", [(None, "2025-03-10"), (pd.NA, "2025-03-10"), ("N123AA", "2025-03-99"), ("N123AA", "2025-02-01")]
)
def test_unassignable_barrier_is_conservative(tail, date):
    barriers, blocked = sequence_barriers(pd.DataFrame([{"tail_number": tail, "flight_date": date}]), "2025-03")
    assert blocked and not barriers


def test_cached_snapshot_and_tamper(cfg, tmp_path):
    a = prepare(cfg, tmp_path, [row(), second()])
    b = prepare(cfg, tmp_path, [row(), second()])
    assert a["version_id"] == b["version_id"]
    (cfg.root / a["silver_relative_path"]).write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="checksum"):
        prepare(cfg, tmp_path, [row(), second()])


def test_changed_source_keeps_key(cfg, tmp_path):
    a = prepare(cfg, tmp_path, [row()], name="a.zip")
    b = prepare(cfg, tmp_path, [row(Tail_Number="N555AA")], name="b.zip")
    assert a["version_id"] != b["version_id"]
    x = pd.read_parquet(cfg.root / a["silver_relative_path"])
    y = pd.read_parquet(cfg.root / b["silver_relative_path"])
    assert x.flight_id.iloc[0] == y.flight_id.iloc[0] and x.record_hash.iloc[0] != y.record_hash.iloc[0]


def test_schema_additions_and_removals(tmp_path):
    assert "NewBTSColumn" in inspect_source(zip_rows(tmp_path, [row(NewBTSColumn="new")]))["added_columns"]
    old = row()
    del old["ArrDelay"]
    with pytest.raises(ValueError, match="schema"):
        inspect_source(zip_rows(tmp_path, [old], "missing.zip"))


@pytest.mark.parametrize("name", ["../bad.csv", "/bad.csv", "x\\bad.csv", "C:bad.csv"])
def test_unsafe_paths(tmp_path, name):
    p = tmp_path / "bad.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr(name, "bad")
    with pytest.raises(ValueError, match="Unsafe"):
        validate_zip(p)


def test_crc_failure(tmp_path):
    p = tmp_path / "crc.zip"
    with zipfile.ZipFile(p, "w", zipfile.ZIP_STORED) as z:
        z.writestr("source.csv", "unique payload")
    p.write_bytes(p.read_bytes().replace(b"unique payload", b"UNIQUE payload"))
    with pytest.raises(zipfile.BadZipFile):
        validate_zip(p)


def test_immutable_collision(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_bytes(b"old")
    put_immutable(a, b)
    a.write_bytes(b"new")
    with pytest.raises(ValueError, match="Immutable"):
        put_immutable(a, b)
    assert b.read_bytes() == b"old"


class Response:
    def __init__(self, status, data=b"", headers=None):
        self.status_code = status
        self.data = data
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def iter_content(self, size):
        yield self.data


class Session:
    def __init__(self, items):
        self.items = iter(items)
        self.calls = 0

    def get(self, *a, **k):
        self.calls += 1
        v = next(self.items)
        if isinstance(v, Exception):
            raise v
        return v


def test_429_timeout_success(tmp_path):
    p = zip_rows(tmp_path, [row()])
    s = Session([Response(429, headers={"Retry-After": "1"}), requests.Timeout(), Response(200, p.read_bytes())])
    waits = []
    m = download_zip("https://example.test", tmp_path / "out.zip", session=s, sleeper=waits.append)
    assert s.calls == 3 and waits == [1, 2] and m["sha256"] == sha256(p)


def test_incomplete_download(tmp_path):
    p = zip_rows(tmp_path, [row()])
    r = Response(200, p.read_bytes(), {"Content-Length": str(p.stat().st_size + 1)})
    with pytest.raises(IncompleteDownload):
        download_zip("https://example.test", tmp_path / "out.zip", session=Session([r]), attempts=1)
    assert not (tmp_path / "out.zip").exists() and not (tmp_path / "out.partial").exists()


def test_404_not_retried(tmp_path):
    s = Session([Response(404)])
    with pytest.raises(requests.HTTPError):
        download_zip("https://example.test", tmp_path / "out.zip", session=s)
    assert s.calls == 1


def test_html_rejected(tmp_path):
    with pytest.raises(zipfile.BadZipFile):
        download_zip(
            "https://example.test",
            tmp_path / "out.zip",
            session=Session([Response(200, b"<html>error</html>")]),
            attempts=1,
        )


def test_month_range_and_mode_guard(cfg):
    assert month_range("2024-12", "2025-02") == ("2024-12", "2025-01", "2025-02")
    with pytest.raises(ValueError):
        month_range("2025-02", "2025-01")
    with pytest.raises(ValueError):
        Config(cfg.root, mode="demo").initialize()


def test_atomic_json(tmp_path):
    p = tmp_path / "state.json"
    write_json(p, {"a": 1})
    write_json(p, {"a": 2})
    assert read_json(p) == {"a": 2} and not list(tmp_path.glob("*.partial"))


def test_s3_disabled_by_default(tmp_path, monkeypatch):
    monkeypatch.delenv("S3_BUCKET", raising=False)
    assert mirror_s3(tmp_path / "not-read", "key") is None


def test_invalid_archive_retried_before_promotion(tmp_path):
    p = zip_rows(tmp_path, [row()])
    waits = []
    s = Session([Response(200, b"truncated ZIP"), Response(200, p.read_bytes())])
    r = download_zip("https://example.test", tmp_path / "retried.zip", session=s, sleeper=waits.append)
    assert s.calls == 2 and waits == [1] and r["sha256"] == sha256(p)

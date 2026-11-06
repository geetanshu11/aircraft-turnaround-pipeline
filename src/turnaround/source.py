"""Monthly HTTP ingestion, strengthened from the legacy aircraft_ingest.py."""

from pathlib import Path, PurePosixPath
import csv
import io
import os
import stat
import time
import zipfile
import requests
from .core import digest, now, sha256
from .transform import COLUMNS


class IncompleteDownload(ValueError):
    pass


def validate_zip(path, full=True):
    with zipfile.ZipFile(path) as z:
        if len(z.namelist()) != len(set(z.namelist())):
            raise ValueError("Duplicate ZIP members")
        if sum(i.file_size for i in z.infolist()) > 4 * 1024**3:
            raise ValueError("ZIP exceeds expanded size limit")
        for i in z.infolist():
            p = PurePosixPath(i.filename)
            if (
                p.is_absolute()
                or ".." in p.parts
                or "\\" in i.filename
                or ":" in i.filename
                or stat.S_ISLNK(i.external_attr >> 16)
            ):
                raise ValueError("Unsafe ZIP path")
        files = [i for i in z.infolist() if i.filename.lower().endswith(".csv")]
        if len(files) != 1 or files[0].file_size == 0:
            raise ValueError("Expected exactly one nonempty CSV")
        if full and z.testzip() is not None:
            raise zipfile.BadZipFile("ZIP CRC failure")
        return files[0].filename


def inspect_source(path):
    member = validate_zip(path)
    with zipfile.ZipFile(path) as z, z.open(member) as f:
        header = next(csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig")))
    if len(header) != len(set(header)):
        raise ValueError("Duplicate CSV columns")
    missing = sorted(set(COLUMNS) - set(header))
    if missing:
        raise ValueError(f"BTS schema missing columns: {missing}")
    return {
        "csv_member": member,
        "columns": header,
        "schema_sha256": digest(header),
        "added_columns": sorted(set(header) - set(COLUMNS)),
    }


def download_zip(url, target, session=None, sleeper=time.sleep, attempts=4):
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(".partial")
    owns_session = session is None
    session = session or requests.Session()
    try:
        for attempt in range(attempts):
            try:
                with session.get(
                    url,
                    stream=True,
                    timeout=(20, 90),
                    headers={"User-Agent": "AircraftTurnaroundPortfolio/2.0", "Accept-Encoding": "identity"},
                ) as response:
                    if response.status_code in {429, 500, 502, 503, 504}:
                        if attempt == attempts - 1:
                            response.raise_for_status()
                        retry = response.headers.get("Retry-After", "")
                        sleeper(min(float(retry) if retry.isdigit() else 2**attempt, 60))
                        continue
                    response.raise_for_status()
                    total = 0
                    with partial.open("wb") as f:
                        for chunk in response.iter_content(1024 * 1024):
                            if not chunk:
                                continue
                            total += len(chunk)
                            if total > 200 * 1024**2:
                                raise ValueError("Download exceeds 200 MiB limit")
                            f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno())
                    length = response.headers.get("Content-Length")
                    if length and not response.headers.get("Content-Encoding") and total != int(length):
                        raise IncompleteDownload(f"Content-Length mismatch: expected {length}, got {total}")
                    validate_zip(partial)
                    os.replace(partial, target)
                    return {
                        "url": url,
                        "received_at": now(),
                        "sha256": sha256(target),
                        "bytes": total,
                        "etag": response.headers.get("ETag"),
                        "last_modified": response.headers.get("Last-Modified"),
                    }
            except (
                requests.Timeout,
                requests.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                IncompleteDownload,
                zipfile.BadZipFile,
            ):
                if attempt == attempts - 1:
                    raise
                sleeper(min(2**attempt, 60))
            finally:
                partial.unlink(missing_ok=True)
        raise RuntimeError("Download retry budget exhausted")
    finally:
        if owns_session:
            session.close()

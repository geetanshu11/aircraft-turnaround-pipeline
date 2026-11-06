"""Content-addressed filesystem storage; optional S3-compatible archive mirroring."""

from pathlib import Path
import os
import shutil
import uuid
from .core import sha256


def put_immutable(source, destination):
    source, destination = Path(source), Path(destination)
    expected = sha256(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if sha256(destination) != expected:
            raise ValueError("Immutable artifact collision or corruption")
        return expected
    temp = destination.with_name(destination.name + "." + uuid.uuid4().hex + ".partial")
    try:
        shutil.copyfile(source, temp)
        if sha256(temp) != expected:
            raise ValueError("Copy checksum mismatch")
        # All platform writers hold a shared filesystem lock.
        if destination.exists():
            if sha256(destination) != expected:
                raise ValueError("Immutable artifact collision")
        else:
            os.replace(temp, destination)
    finally:
        temp.unlink(missing_ok=True)
    return expected


def mirror_s3(path, key, client=None):
    """Opt-in mirror: never creates a bucket or discovers credentials outside the SDK chain."""
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        return None
    import boto3
    from botocore.exceptions import ClientError
    import hashlib

    client = client or boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT_URL") or None)
    path = Path(path)
    checksum = sha256(path)
    key = os.getenv("S3_PREFIX", "aircraft").strip("/") + "/" + key
    try:
        with path.open("rb") as body:
            client.put_object(Bucket=bucket, Key=key, Body=body, Metadata={"sha256": checksum}, IfNoneMatch="*")
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
            raise
        response = client.get_object(Bucket=bucket, Key=key)
        h = hashlib.sha256()
        try:
            for block in response["Body"].iter_chunks(1024 * 1024):
                h.update(block)
        finally:
            response["Body"].close()
        if h.hexdigest() != checksum:
            raise ValueError("Existing S3 object checksum mismatch") from exc
    return f"s3://{bucket}/{key}"

"""Resumable single-source collector feeding the frozen analysis kernel.

A download is a contiguous byte prefix persisted under one strong ETag.  The
prefix is flushed and fsynced *before* its length is recorded in SQLite, so a
crash can never advertise bytes that are not on disk.  On the next attempt the
recorded prefix is continued with ``Range``/``If-Range``; a changed validator
discards the old prefix instead of mixing representations.
"""
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urlsplit

import runtime

SCHEMA = """
CREATE TABLE downloads(
 id TEXT PRIMARY KEY, url TEXT NOT NULL, state TEXT NOT NULL,
 etag TEXT, length INTEGER, sha256 TEXT, received INTEGER NOT NULL DEFAULT 0,
 error TEXT, receipt TEXT
);
"""

CHUNK = 16384
FIELDS = ("task_id", "url", "state", "etag", "length", "sha256", "received", "error", "receipt")


def init(root):
    (Path(root) / "downloads").mkdir(exist_ok=True)
    with runtime.connect(root) as db:
        db.executescript(SCHEMA)


def enqueue(root, task_id, url):
    if not isinstance(task_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", task_id):
        raise ValueError("TASK_ID")
    parts = urlsplit(url)
    if parts.scheme != "http" or not parts.hostname or parts.username or parts.password or parts.fragment:
        raise ValueError("URL")
    with runtime.connect(root) as db:
        db.execute("BEGIN IMMEDIATE")
        old = db.execute("SELECT url FROM downloads WHERE id=?", (task_id,)).fetchone()
        if old:
            if old[0] != url:
                raise ValueError("TASK_CONFLICT")
        else:
            if db.execute("SELECT 1 FROM downloads WHERE state NOT IN ('completed','failed')").fetchone():
                raise ValueError("ACTIVE_DOWNLOAD")
            db.execute("INSERT INTO downloads(id,url,state) VALUES(?,?,'queued')", (task_id, url))
    return {"task_id": task_id, "url": url}


# ---------------------------------------------------------------------------
# Small protocol helpers


def _parts(url):
    parts = urlsplit(url)
    return parts, parts.path + ("?" + parts.query if parts.query else "")


def _connect(url, connection_factory=None):
    parts, _ = _parts(url)
    factory = connection_factory or http.client.HTTPConnection
    return factory(parts.hostname, parts.port or 80, timeout=10)


def _request(url, method, headers, connection_factory=None):
    connection = _connect(url, connection_factory)
    _, path = _parts(url)
    try:
        connection.request(method, path, headers={"Accept-Encoding": "identity", **headers})
        return connection, connection.getresponse()
    except BaseException:
        connection.close()
        raise


def _validator(response):
    etag = response.getheader("ETag")
    digest = response.getheader("X-Content-SHA256")
    if not etag or not re.fullmatch(r'"[^"]+"', etag):
        raise ValueError("ETAG")
    if not digest or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("METADATA")
    return etag, digest


def _full_metadata(response):
    etag, digest = _validator(response)
    length_text = response.getheader("Content-Length", "")
    if not length_text.isdecimal():
        raise ValueError("METADATA")
    return {"etag": etag, "length": int(length_text), "sha256": digest}


def _part_metadata(response):
    etag, digest = _validator(response)
    return {"etag": etag, "sha256": digest}


def _head(url, connection_factory=None):
    connection, response = _request(url, "HEAD", {}, connection_factory)
    try:
        response.read()
        if response.status != 200:
            raise ValueError("HTTP_STATUS")
        return _full_metadata(response)
    finally:
        connection.close()


def _hash_file(path):
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while True:
            block = source.read(1 << 20)
            if not block:
                break
            size += len(block)
            digest.update(block)
    return size, digest.hexdigest()


def _verify_file(path, length, digest):
    if not path.exists():
        return False
    size, actual = _hash_file(path)
    return size == length and actual == digest


# ---------------------------------------------------------------------------
# Standalone full download (kept for the recorded protocol regression).


def download(url, target, progress=None, connection_factory=None):
    connection, response = _request(url, "GET", {}, connection_factory)
    try:
        if response.status != 200:
            raise ValueError("HTTP_STATUS")
        metadata = _full_metadata(response)
        size, actual = 0, hashlib.sha256()
        while True:
            block = response.read(CHUNK)
            if not block:
                break
            target.write(block)
            size += len(block)
            actual.update(block)
            if progress:
                progress(size, metadata)
        if size != metadata["length"]:
            raise OSError("SHORT_RESPONSE")
        if actual.hexdigest() != metadata["sha256"]:
            raise ValueError("DIGEST")
        return metadata
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Durable checkpoint helpers.  Every function commits before returning; no
# network operation is ever performed while one of these transactions is open.


def _row(root, name):
    with runtime.connect(root) as db:
        row = db.execute("SELECT received,etag,length,sha256 FROM downloads WHERE id=?",
                         (name,)).fetchone()
    return {"received": row[0], "etag": row[1], "length": row[2], "sha256": row[3]}


def _begin(root, name):
    with runtime.connect(root) as db:
        db.execute("UPDATE downloads SET state='downloading',error=NULL WHERE id=?", (name,))


def _store(root, name, metadata, received):
    with runtime.connect(root) as db:
        db.execute("UPDATE downloads SET received=?,etag=?,length=?,sha256=? WHERE id=?",
                   (received, metadata["etag"], metadata["length"], metadata["sha256"], name))


def _reset(root, name):
    with runtime.connect(root) as db:
        db.execute("UPDATE downloads SET received=0,etag=NULL,length=NULL,sha256=NULL WHERE id=?",
                   (name,))


def _fail(root, name, state, message):
    with runtime.connect(root) as db:
        db.execute("UPDATE downloads SET state=?,error=? WHERE id=?", (state, message, name))


# ---------------------------------------------------------------------------
# Transfer


def _stream(response, target, root, name, metadata, base):
    """Write the response, making every chunk durable before confirming it."""
    written = 0
    while True:
        block = response.read(CHUNK)
        if not block:
            break
        target.write(block)
        target.flush()
        os.fsync(target.fileno())
        written += len(block)
        received = base + written
        if received > metadata["length"]:
            raise ValueError("LENGTH")
        _store(root, name, metadata, received)
        runtime.barrier(root, "chunk")
    return written


def _verified(root, name, part, metadata, raw):
    """Move a fully verified part into place atomically and hand it to runtime."""
    if not _verify_file(part, metadata["length"], metadata["sha256"]):
        raise ValueError("DIGEST")
    os.replace(part, raw)
    runtime.sync_directory(raw.parent)
    _store(root, name, metadata, metadata["length"])
    return metadata


def _fetch_full(root, name, url, part, connection_factory=None):
    with part.open("wb") as target:
        connection, response = _request(url, "GET", {}, connection_factory)
        try:
            if response.status != 200:
                raise ValueError("HTTP_STATUS")
            metadata = _full_metadata(response)
            _store(root, name, metadata, 0)
            written = _stream(response, target, root, name, metadata, 0)
        finally:
            connection.close()
    if written != metadata["length"]:
        raise OSError("SHORT_RESPONSE")
    return metadata


def _fetch_resume(root, name, url, part, checkpoint, connection_factory=None):
    offset = checkpoint["received"]
    etag = checkpoint["etag"]
    expected = {"etag": etag, "length": checkpoint["length"], "sha256": checkpoint["sha256"]}
    connection, response = _request(url, "GET", {"Range": "bytes=%d-" % offset,
                                                 "If-Range": etag}, connection_factory)
    try:
        if response.status == 206:
            content_range = response.getheader("Content-Range", "")
            match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
            metadata = _part_metadata(response)
            if (not match or int(match[1]) != offset or int(match[3]) != expected["length"]
                    or int(match[2]) != expected["length"] - 1
                    or metadata["etag"] != etag or metadata["sha256"] != expected["sha256"]):
                # Unexpected range/representation: restart from byte zero.
                connection.close()
                _reset(root, name)
                part.unlink(missing_ok=True)
                return _fetch_full(root, name, url, part, connection_factory)
            metadata = {**expected, "etag": metadata["etag"], "sha256": metadata["sha256"]}
            with part.open("r+b") as target:
                target.seek(offset)
                target.truncate(offset)
                written = _stream(response, target, root, name, metadata, offset)
            if written != expected["length"] - offset:
                raise OSError("SHORT_RESPONSE")
            return metadata
        if response.status == 200:
            # The representation changed between HEAD and GET.  The old prefix
            # belongs to the previous version and must not be appended to.
            metadata = _full_metadata(response)
            _reset(root, name)
            with part.open("wb") as target:
                _store(root, name, metadata, 0)
                written = _stream(response, target, root, name, metadata, 0)
            if written != metadata["length"]:
                raise OSError("SHORT_RESPONSE")
            return metadata
        if response.status == 416 and offset >= expected["length"]:
            if _verify_file(part, expected["length"], expected["sha256"]):
                return expected
            _reset(root, name)
            part.unlink(missing_ok=True)
            return _fetch_full(root, name, url, part, connection_factory)
        raise ValueError("HTTP_STATUS")
    finally:
        connection.close()


def _handoff(root, name, raw, metadata):
    runtime.barrier(root, "verified")
    batch = json.loads(raw.read_bytes())
    runtime.submit(root, batch)
    runtime.barrier(root, "accepted")
    runtime.run_one(root)
    runtime.barrier(root, "accounted")
    result = runtime.read(root, batch["batch_id"])
    if not result["available"]:
        raise ValueError("NOT_ACCOUNTED")
    with runtime.connect(root) as db:
        db.execute("UPDATE downloads SET state='completed',received=?,receipt=?,error=NULL WHERE id=?",
                   (metadata["length"], json.dumps(result["receipt"], sort_keys=True), name))


def _finish(root, name, part, raw, metadata, connection_factory=None):
    """Complete a part that already holds the whole representation."""
    _verified(root, name, part, metadata, raw)
    return _handoff(root, name, raw, metadata)


def _collect(root, name, url, checkpoint, connection_factory=None):
    part = root / "downloads" / (name + ".part")
    raw = root / "downloads" / (name + ".raw")

    # A .raw file only exists after the atomic move of a verified part, so its
    # presence means the hand-off was interrupted, not the transfer.
    if (checkpoint["etag"] is not None and checkpoint["length"] is not None
            and checkpoint["sha256"] is not None
            and _verify_file(raw, checkpoint["length"], checkpoint["sha256"])):
        metadata = {"etag": checkpoint["etag"], "length": checkpoint["length"],
                    "sha256": checkpoint["sha256"]}
        return _handoff(root, name, raw, metadata)
    raw.unlink(missing_ok=True)

    received = checkpoint["received"] or 0
    complete = (checkpoint["etag"] is not None and checkpoint["length"] is not None
                and checkpoint["sha256"] is not None)

    if complete and part.exists() and received:
        actual = part.stat().st_size
        if actual < received:
            # The durable file is shorter than the confirmed length: the
            # prefix cannot be trusted, so start over rather than blend bytes.
            _reset(root, name)
            part.unlink(missing_ok=True)
            complete, received = False, 0
        elif actual > received:
            # Bytes past the last confirmed point were never acknowledged.
            with part.open("r+b") as target:
                target.truncate(received)

    if complete and received and part.exists():
        head = _head(url, connection_factory)
        if (head["etag"] != checkpoint["etag"] or head["length"] != checkpoint["length"]
                or head["sha256"] != checkpoint["sha256"]):
            # Version changed while incomplete: drop the old prefix.
            _reset(root, name)
            part.unlink(missing_ok=True)
        elif received == checkpoint["length"]:
            metadata = {"etag": checkpoint["etag"], "length": checkpoint["length"],
                        "sha256": checkpoint["sha256"]}
            return _finish(root, name, part, raw, metadata, connection_factory)
        else:
            metadata = _fetch_resume(root, name, url, part, checkpoint, connection_factory)
            return _finish(root, name, part, raw, metadata, connection_factory)

    _reset(root, name)
    part.unlink(missing_ok=True)
    metadata = _fetch_full(root, name, url, part, connection_factory)
    return _finish(root, name, part, raw, metadata, connection_factory)


def run_one(root):
    root = Path(root)
    with runtime.connect(root) as db:
        row = db.execute("SELECT id,url FROM downloads WHERE state IN ('queued','downloading') "
                         "ORDER BY id LIMIT 1").fetchone()
        if row is None:
            return False
        name, url = row
    _begin(root, name)
    checkpoint = _row(root, name)
    try:
        _collect(root, name, url, checkpoint)
    except (OSError, http.client.HTTPException) as error:
        _fail(root, name, "queued", str(error))
    except ValueError as error:
        _fail(root, name, "failed", str(error))
    return True


def status(root):
    with runtime.connect(root) as db:
        rows = db.execute("SELECT id,url,state,etag,length,sha256,received,error,receipt "
                          "FROM downloads ORDER BY id").fetchall()
    result = [dict(zip(FIELDS, row)) for row in rows]
    for row in result:
        row["receipt"] = json.loads(row["receipt"]) if row["receipt"] else None
    return result


def worker(root):
    import fcntl
    with (Path(root) / "worker.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            if not run_one(root):
                runtime.run_one(root)
            time.sleep(0.2)

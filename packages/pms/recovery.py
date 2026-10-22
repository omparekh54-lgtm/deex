"""Restore rehearsal guards. Never restore into an existing application database."""

from __future__ import annotations
import hashlib
import os
import subprocess
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.engine import make_url, URL

OBJECTS_SQL = """select count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace
where n.nspname not in ('pg_catalog','information_schema') and n.nspname not like 'pg_toast%'
and c.relkind in ('r','p','v','m','S') and not exists(select 1 from pg_depend d where d.objid=c.oid and d.deptype='e')"""


def archive_sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def endpoint_fingerprint(url):
    parsed = make_url(url)
    return hashlib.sha256(
        repr(
            (parsed.host, parsed.port or 5432, parsed.database, parsed.username)
        ).encode()
    ).hexdigest()


def inspect_archive(path, expected):
    if len(expected) != 64 or archive_sha(path) != expected.lower():
        raise ValueError("Backup checksum mismatch")
    result = subprocess.run(
        ["pg_restore", "--list", str(Path(path).resolve())],
        capture_output=True,
        check=False,
        timeout=60,
    )
    if result.returncode:
        raise RuntimeError("Backup archive cannot be inspected")
    lines = result.stdout.decode("utf-8", errors="replace").splitlines()
    entries = sum(bool(line.strip()) and not line.startswith(";") for line in lines)
    if not entries:
        raise ValueError("Empty backup archive")
    return dict(
        archive_sha256=expected.lower(),
        toc_entries=entries,
        status="ARCHIVE_INSPECTED",
        restored=False,
    )


def restore_rehearsal(path, expected, source_url, target_url, target_engine, *, verify):
    report = inspect_archive(path, expected)
    if endpoint_fingerprint(source_url) == endpoint_fingerprint(target_url):
        raise ValueError("Restore target must be isolated from source")
    with target_engine.connect() as conn:
        if conn.execute(text(OBJECTS_SQL)).scalar_one() != 0:
            raise ValueError("Restore target contains application objects")
    target = make_url(target_url)
    env = os.environ.copy()
    if target.password is not None:
        env["PGPASSWORD"] = target.password
    else:
        env.pop("PGPASSWORD", None)
    # No password is placed in argv or emitted from captured pg_restore errors.
    allowed = {
        "sslmode",
        "sslrootcert",
        "sslcert",
        "sslkey",
        "connect_timeout",
        "application_name",
    }
    if set(target.query) - allowed:
        raise ValueError("Unsupported restore connection option")
    dsn = URL.create(
        "postgresql",
        username=target.username,
        host=target.host,
        port=target.port,
        database=target.database,
        query=target.query,
    ).render_as_string(hide_password=False)
    result = subprocess.run(
        [
            "pg_restore",
            "--exit-on-error",
            "--single-transaction",
            "--no-owner",
            "--no-privileges",
            "--dbname",
            dsn,
            str(Path(path).resolve()),
        ],
        env=env,
        capture_output=True,
        check=False,
        timeout=3600,
    )
    if result.returncode:
        raise RuntimeError("Restore failed; isolated target must be investigated")
    # Require exact source manifest comparison; DDL restore alone is not recovery proof.
    if verify(target_engine) is not True:
        raise ValueError("Restored data does not match the source audit manifest")
    return {
        **report,
        "status": "RESTORE_MANIFEST_MATCHED",
        "restored": True,
        "target_fingerprint": endpoint_fingerprint(target_url),
        "acceptance": "NOT_CERTIFIED",
        "note": "Data manifest rehearsal only; roles, grants, secrets, deployment and recovery-time objectives require separate review.",
    }

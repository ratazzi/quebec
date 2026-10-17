from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import TypeVar

import sqlalchemy.exc
from sqlalchemy import create_engine, text

T = TypeVar("T")


@contextmanager
def database_engine(db_url: str, *, readonly: bool = False):
    """Open a verification engine, preserving PostgreSQL connection options."""
    engine = create_database_engine(db_url, readonly=readonly)
    try:
        yield engine
    finally:
        engine.dispose()


def insert_returning_id(session, sql: str, params: dict | None = None) -> int:
    """Run a single-row INSERT and return the new row's id on every backend.

    MySQL has no ``INSERT ... RETURNING``, so it reads the driver's lastrowid.
    """
    if session.get_bind().dialect.name == "mysql":
        return session.execute(text(sql), params).lastrowid
    return session.execute(text(f"{sql} RETURNING id"), params).scalar_one()


def drop_jobs_batch_id(session, prefix: str) -> None:
    """Strip ``jobs.batch_id`` to mimic a schema that predates batches.

    MySQL has no ``DROP INDEX IF EXISTS`` and drops a column's single-column
    index together with the column, so the explicit index drop is skipped there.
    """
    if session.get_bind().dialect.name != "mysql":
        session.execute(text(f"DROP INDEX IF EXISTS idx_{prefix}_jobs_batch_id"))
    session.execute(text(f"ALTER TABLE {prefix}_jobs DROP COLUMN batch_id"))


def create_database_engine(db_url: str, *, readonly: bool = False):
    """Create an observer engine; the caller must dispose it after use.

    MySQL URLs are pinned to PyMySQL (``mysql://`` would pick MySQLdb) and to
    READ COMMITTED: the default REPEATABLE READ would pin a long-lived observer
    session to its first snapshot and hide every later Quebec commit. ANSI_QUOTES
    lets test SQL quote identifiers (``"key"``) the same way on every backend.
    """
    if db_url.startswith("sqlite:"):
        path = db_url.removeprefix("sqlite:///").split("?", 1)[0]
        db_url = f"sqlite:///file:{path}?mode=ro&uri=true" if readonly else f"sqlite:///{path}"
    if db_url.startswith("mysql://"):
        return create_engine(
            db_url.replace("mysql://", "mysql+pymysql://", 1),
            isolation_level="READ COMMITTED",
            connect_args={"init_command": "SET SESSION sql_mode = CONCAT(@@sql_mode, ',ANSI_QUOTES')"},
        )
    return create_engine(db_url)

# Python's ``sqlite3`` and Quebec's Rust sqlite are two independent SQLite
# libraries inside one process. POSIX advisory locks are owned by the process,
# so neither library can see the other's file locks: a test connection reading
# a database Quebec is writing periodically mistakes Quebec's in-flight
# rollback journal for a hot journal. A writable connection then *replays* it,
# silently undoing Quebec's transaction; a read-only one refuses instead and
# raises one of the errors below, which clear as soon as the journal is gone.
_TRANSIENT_SQLITE_ERRORS = (
    "attempt to write a readonly database",
    "disk I/O error",
    "database is locked",
)


def get_job_by_active_job_id(session, prefix: str, active_job_id: str) -> dict:
    result = session.execute(
        text(f"SELECT * FROM {prefix}_jobs WHERE active_job_id = :active_job_id"),
        {"active_job_id": active_job_id},
    )
    row = result.fetchone()
    if row is None:
        raise AssertionError(f"Job with active_job_id={active_job_id!r} was not found")
    return dict(row._mapping)


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 5.0,
    interval: float = 0.05,
    message: str = "Timed out waiting for condition",
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(message)


def readonly_connect(db_path: str) -> sqlite3.Connection:
    """Open an observer connection on a database Quebec is actively writing.

    Read-only so it can never replay - and thereby roll back - a transaction
    Quebec has in flight. Pair every query with :func:`observe_sqlite`.
    """
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def observe_sqlite(
    query: Callable[[], T], *, attempts: int = 50, delay: float = 0.01
) -> T:
    """Run a read-only query against a database Quebec is actively writing."""
    for attempt in range(1, attempts + 1):
        try:
            return query()
        except (sqlite3.OperationalError, sqlalchemy.exc.OperationalError) as exc:
            transient = any(msg in str(exc) for msg in _TRANSIENT_SQLITE_ERRORS)
            if not transient or attempt == attempts:
                raise
            time.sleep(delay)

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from typing import TypeVar

import sqlalchemy.exc
from sqlalchemy import text

T = TypeVar("T")

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

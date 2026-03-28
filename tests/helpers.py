from __future__ import annotations

import time
from collections.abc import Callable

from sqlalchemy import text


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

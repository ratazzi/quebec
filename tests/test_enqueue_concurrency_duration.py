"""Single and bulk enqueue must preserve the class's semaphore TTL."""

from datetime import datetime, timedelta

import pytest
import quebec
from sqlalchemy import text


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("duration", [None, 3, 3600, timedelta(hours=1)])
def test_enqueue_preserves_concurrency_duration(qc_with_sqlalchemy, bulk, duration):
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    class DurationJob(quebec.BaseClass):
        concurrency_limit = 1

        def concurrency_key(self, value):
            return str(value)

        def perform(self, value):
            pass

    if duration is not None:
        DurationJob.concurrency_duration = duration
    qc.register_job(DurationJob)

    if bulk:
        qc.perform_all_later([DurationJob.build(1), DurationJob.build(1)])
    else:
        DurationJob.perform_later(qc, 1)
        DurationJob.perform_later(qc, 1)

    seconds = duration.total_seconds() if isinstance(duration, timedelta) else duration
    expected_ttls = {
        "semaphores": 120 if seconds is None else seconds,
        "blocked_executions": 60 if seconds is None else seconds,
    }
    for table, expected in expected_ttls.items():
        row = session.execute(text(
            f"SELECT expires_at, created_at FROM {prefix}_{table}"
        )).one()
        expires = datetime.fromisoformat(row.expires_at)
        created = datetime.fromisoformat(row.created_at)
        assert abs((expires - created).total_seconds() - expected) < 1

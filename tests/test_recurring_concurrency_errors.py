"""Recurring enqueue must not turn a broken concurrency key into unrestricted work."""

import pytest
import quebec
from sqlalchemy import text


@pytest.mark.parametrize("failure", ["raise", "invalid_type"])
def test_recurring_rejects_invalid_concurrency_key(qc_with_sqlalchemy, failure):
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    class BrokenKeyJob(quebec.BaseClass):
        concurrency_limit = 1

        def concurrency_key(self):
            if failure == "raise":
                raise ValueError("cannot resolve concurrency key")
            return 123

        def perform(self):
            pass

    qc.register_job(BrokenKeyJob)
    session.execute(text(
        f"INSERT INTO {prefix}_recurring_tasks "
        '(key, schedule, class_name, arguments, queue_name, priority, "static", created_at, updated_at) '
        "VALUES ('broken', 'every minute', :class_name, '[]', 'default', 0, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    ), {"class_name": BrokenKeyJob.__qualname__})
    session.commit()

    with pytest.raises(RuntimeError, match="concurrency"):
        qc.run_recurring_now("broken")

    for table in ("jobs", "recurring_executions", "ready_executions", "blocked_executions", "semaphores"):
        count = session.execute(text(f"SELECT COUNT(*) FROM {prefix}_{table}")).scalar()
        assert count == 0, f"failed key resolution left a row in {table}"

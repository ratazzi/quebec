"""A surviving PostgreSQL scheduler keeps recurring ticks contiguous."""

import os
import tempfile
import time
from pathlib import Path

import pytest
import quebec
from sqlalchemy import text


class PgScheduledWork(quebec.BaseClass):
    queue_as = "perf"

    def perform(self, value=None):
        return None


def test_postgres_scheduler_continues_after_peer_closes(qc_with_sqlalchemy):
    if qc_with_sqlalchemy["db_url"].startswith("sqlite:"):
        pytest.skip("PostgreSQL scheduler failover case")
    qc = qc_with_sqlalchemy["qc"]
    db_url = qc_with_sqlalchemy["db_url"]
    prefix = qc_with_sqlalchemy["prefix"]
    engine = qc_with_sqlalchemy["engine"]
    second = quebec.Quebec(db_url, table_name_prefix=prefix, use_listen_notify=False)
    old_schedule = os.environ.get("QUEBEC_RECURRING_SCHEDULE")
    old_env = os.environ.get("QUEBEC_ENV")
    schedule_path = None
    second_closed = False

    def count(table):
        with engine.connect() as observer:
            return observer.execute(
                text(f"SELECT count(*) FROM {prefix}_{table}")
            ).scalar_one()

    def wait_for_ticks(target):
        deadline = time.perf_counter() + 8
        while count("recurring_executions") < target:
            if time.perf_counter() >= deadline:
                raise AssertionError(
                    f"PostgreSQL scheduler did not reach tick {target}"
                )
            time.sleep(0.02)

    try:
        qc.register_job(PgScheduledWork)
        second.register_job(PgScheduledWork)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as file:
            file.write(
                "test:\n  tick:\n    class: PgScheduledWork\n"
                "    schedule: every second\n    queue: perf\n"
            )
            schedule_path = file.name
        os.environ["QUEBEC_RECURRING_SCHEDULE"] = schedule_path
        os.environ["QUEBEC_ENV"] = "test"
        qc.spawn_scheduler()
        second.spawn_scheduler()
        wait_for_ticks(2)
        second.close()
        second_closed = True
        at_close = count("recurring_executions")
        wait_for_ticks(at_close + 3)
        with engine.connect() as observer:
            run_ats = (
                observer.execute(
                    text(
                        f"SELECT run_at FROM {prefix}_recurring_executions ORDER BY run_at"
                    )
                )
                .scalars()
                .all()
            )
        assert all(
            (right - left).total_seconds() == 1
            for left, right in zip(run_ats, run_ats[1:])
        )
        assert len(run_ats) == count("jobs") == count("ready_executions")
    finally:
        if not second_closed:
            second.close()
        if schedule_path is not None:
            Path(schedule_path).unlink()
        if old_schedule is None:
            os.environ.pop("QUEBEC_RECURRING_SCHEDULE", None)
        else:
            os.environ["QUEBEC_RECURRING_SCHEDULE"] = old_schedule
        if old_env is None:
            os.environ.pop("QUEBEC_ENV", None)
        else:
            os.environ["QUEBEC_ENV"] = old_env

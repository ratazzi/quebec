"""Enqueue hooks on the scheduler/recurring enqueue path.

The scheduler runs before/around/after_enqueue hooks (arbitrary user Python)
around its DB writes. Those hooks must run OUTSIDE the enqueue transaction —
holding the transaction across them deadlocks SQLite and widens Postgres
row-lock contention. These tests fire the exact scheduler enqueue path via
``run_recurring_now`` and assert the hooks are still orchestrated correctly.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import text

import quebec


class RecurringHookJob(quebec.BaseClass):
    calls: list[str] = []

    def before_enqueue(self) -> None:
        type(self).calls.append("before")

    def around_enqueue(self):
        type(self).calls.append("around:before")
        yield
        type(self).calls.append("around:after")

    def after_enqueue(self) -> None:
        type(self).calls.append("after")

    def perform(self, *args, **kwargs) -> None:
        return None


class RecurringSkipHookJob(quebec.BaseClass):
    calls: list[str] = []

    def around_enqueue(self):
        type(self).calls.append("skipped")
        # A generator that never yields -> enqueue is skipped.
        if False:
            yield

    def perform(self, *args, **kwargs) -> None:
        return None


class RecurringDatabaseHookJob(quebec.BaseClass):
    engine = None
    task_table = ""
    task_key = ""

    def after_enqueue(self) -> None:
        engine = type(self).engine
        assert engine is not None

        # Use a separate connection so this write fails immediately on SQLite
        # if the scheduler still holds its enqueue write transaction.
        with engine.begin() as connection:
            if connection.dialect.name == "sqlite":
                connection.exec_driver_sql("PRAGMA busy_timeout = 0")
            result = connection.execute(
                text(
                    f"UPDATE {type(self).task_table} "
                    "SET description = :description WHERE key = :key"
                ),
                {"description": "written-by-after-hook", "key": type(self).task_key},
            )
            assert result.rowcount == 1

    def perform(self, *args, **kwargs) -> None:
        return None


def _seed_recurring_task(session, prefix: str, key: str, class_name: str) -> None:
    """Insert a recurring_tasks row so run_recurring_now() can look it up."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    session.execute(
        text(
            f"INSERT INTO {prefix}_recurring_tasks "
            "(key, schedule, class_name, arguments, queue_name, priority, "
            '"static", created_at, updated_at) '
            "VALUES (:key, :schedule, :class_name, :arguments, :queue_name, "
            ":priority, :static, :created_at, :updated_at)"
        ),
        {
            "key": key,
            "schedule": "every minute",
            "class_name": class_name,
            "arguments": "[]",
            "queue_name": "default",
            "priority": 0,
            "static": True,
            "created_at": now,
            "updated_at": now,
        },
    )
    session.commit()


def test_recurring_enqueue_runs_all_hooks(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    RecurringHookJob.calls = []
    qc.register_job(RecurringHookJob)

    task_key = "recurring_hook_all"
    _seed_recurring_task(session, prefix, task_key, RecurringHookJob.__qualname__)

    assert qc.run_recurring_now(task_key) is True

    # before + around-before run before the DB insert; around-after + after run
    # after it — all outside the enqueue transaction.
    assert RecurringHookJob.calls == [
        "before",
        "around:before",
        "around:after",
        "after",
    ]
    assert db_assert.count_ready_executions() == 1


def test_recurring_enqueue_post_hook_runs_after_transaction_commits(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    engine = qc_with_sqlalchemy["engine"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    task_key = "recurring_hook_db_write"
    RecurringDatabaseHookJob.engine = engine
    RecurringDatabaseHookJob.task_table = f"{prefix}_recurring_tasks"
    RecurringDatabaseHookJob.task_key = task_key
    qc.register_job(RecurringDatabaseHookJob)
    _seed_recurring_task(
        session, prefix, task_key, RecurringDatabaseHookJob.__qualname__
    )

    assert qc.run_recurring_now(task_key) is True

    description = session.execute(
        text(f"SELECT description FROM {prefix}_recurring_tasks WHERE key = :key"),
        {"key": task_key},
    ).scalar_one()
    assert description == "written-by-after-hook"
    assert db_assert.count_ready_executions() == 1


def test_recurring_enqueue_around_no_yield_skips(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    RecurringSkipHookJob.calls = []
    qc.register_job(RecurringSkipHookJob)

    task_key = "recurring_hook_skip"
    _seed_recurring_task(session, prefix, task_key, RecurringSkipHookJob.__qualname__)

    # around_enqueue never yields -> the job is skipped, nothing enqueued.
    assert qc.run_recurring_now(task_key) is False
    assert RecurringSkipHookJob.calls == ["skipped"]
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_jobs() == 0

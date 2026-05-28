"""Regression tests for recurring task + concurrency Discard interaction.

Mirrors the Solid Queue edge case from commit be1a5bc / issue #598: a recurring
task fires, its underlying job is dropped by concurrency control, and Solid
Queue then crashed trying to record a recurring_execution row whose FK pointed
at a job that was never persisted.

Quebec is structurally immune because ``scheduler::enqueue_job`` runs in a
different order than Solid Queue:

1. Insert the job row.
2. Insert the recurring_execution row (FK already valid).
3. Run concurrency control.

When concurrency control discards the job it calls ``mark_finished`` (sets
``finished_at``), it does NOT delete the job row. So the recurring_execution
FK stays valid and the row is correctly recorded against a finished job.

These tests lock that behaviour in.
"""

from __future__ import annotations

from datetime import datetime, timezone

import quebec
from sqlalchemy import text


class RecurringDiscardJob(quebec.BaseClass):
    """Discard-mode job with a fixed concurrency key (singleton)."""

    concurrency_limit = 1
    concurrency_on_conflict = quebec.ConcurrencyConflict.Discard

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "recurring-singleton"

    def perform(self, *args, **kwargs) -> None:
        return None


class RecurringBlockJob(quebec.BaseClass):
    """Block-mode job with a fixed concurrency key (singleton)."""

    concurrency_limit = 1

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "recurring-block-singleton"

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


def test_recurring_discard_with_slot_taken_records_finished_execution(
    qc_with_sqlalchemy, db_assert
) -> None:
    """Recurring task fires while the concurrency slot is taken (Discard mode).

    The underlying job must still be persisted (then marked finished), and the
    recurring_execution row must reference that real, finished job. No crash,
    no orphaned FK.
    """
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(RecurringDiscardJob)

    # Take the singleton slot first with a regular enqueue: this acquires the
    # semaphore so the recurring task that follows will be discarded.
    RecurringDiscardJob.perform_later(qc)
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 0

    task_key = "recurring_discard_singleton"
    _seed_recurring_task(session, prefix, task_key, RecurringDiscardJob.__qualname__)

    # Fire the recurring task. This runs the exact scheduler enqueue path.
    # Returns False because the job was discarded (not actually enqueued),
    # and crucially must NOT raise (the Solid Queue bug shape).
    enqueued = qc.run_recurring_now(task_key)
    assert enqueued is False

    session.expire_all()

    # The recurring_execution row was recorded (insert happens before the
    # discard) and references a real, finished job — FK stays valid.
    rec = session.execute(
        text(
            f"SELECT job_id, task_key FROM {prefix}_recurring_executions "
            "WHERE task_key = :k"
        ),
        {"k": task_key},
    ).fetchone()
    assert rec is not None, "recurring_execution row should be recorded"

    job = db_assert.get_job(rec.job_id)
    assert job is not None, "recurring_execution must reference a real job"
    assert job["finished_at"] is not None, "discarded job must be marked finished"

    # The discarded recurring job did not reach ready/blocked, and did not fail.
    assert db_assert.count_ready_executions() == 1  # only the slot holder
    assert db_assert.count_blocked_executions() == 0
    assert db_assert.count_failed_executions() == 0


def test_recurring_block_with_slot_taken_records_blocked_execution(
    qc_with_sqlalchemy, db_assert
) -> None:
    """Recurring task fires while the slot is taken (Block mode).

    The job is persisted and routed to blocked_executions; the
    recurring_execution row references the same real job.
    """
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(RecurringBlockJob)

    # Take the slot first.
    RecurringBlockJob.perform_later(qc)
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 0

    task_key = "recurring_block_singleton"
    _seed_recurring_task(session, prefix, task_key, RecurringBlockJob.__qualname__)

    # Block mode persists the job (goes to blocked_executions), so the recurring
    # task counts as enqueued.
    enqueued = qc.run_recurring_now(task_key)
    assert enqueued is True

    session.expire_all()

    rec = session.execute(
        text(f"SELECT job_id FROM {prefix}_recurring_executions WHERE task_key = :k"),
        {"k": task_key},
    ).fetchone()
    assert rec is not None

    # The recurring_execution references a real job that is blocked, not finished.
    job = db_assert.get_job(rec.job_id)
    assert job is not None
    assert job["finished_at"] is None

    blocked = session.execute(
        text(f"SELECT job_id FROM {prefix}_blocked_executions WHERE job_id = :j"),
        {"j": rec.job_id},
    ).fetchone()
    assert blocked is not None, "blocked recurring job must reference the same job_id"
    assert db_assert.count_failed_executions() == 0

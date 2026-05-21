"""Tests for the blocked/scheduled/recurring Python API on the Quebec instance."""

from __future__ import annotations

import quebec
from sqlalchemy import text


def _count(session, prefix, table, where=""):
    sql = f"SELECT COUNT(*) AS c FROM {prefix}_{table} {where}".strip()
    return session.execute(text(sql)).scalar()


class ExclusiveJob(quebec.ActiveJob):
    queue_as = "default"
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "exclusive-resource"

    def perform(self, *args, **kwargs):
        pass


class OtherExclusiveJob(quebec.ActiveJob):
    queue_as = "default"
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "other-exclusive"

    def perform(self, *args, **kwargs):
        pass


def _enqueue_blocked_pair(qc, job_cls):
    """Enqueue two of the same concurrency-keyed job; second lands in blocked."""
    first = job_cls.perform_later(qc)
    second = job_cls.perform_later(qc)
    return first, second


def test_unblock_promotes_blocked_execution_to_ready(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ExclusiveJob)
    _first, blocked = _enqueue_blocked_pair(qc, ExclusiveJob)
    session.expire_all()
    assert _count(session, prefix, "blocked_executions") == 1

    assert qc.unblock(blocked.id) is True

    session.expire_all()
    # blocked_execution gone; job row stays and is now in ready_executions
    # with the same queue/priority — i.e. actually unblocked, not orphaned.
    assert _count(session, prefix, "blocked_executions") == 0
    assert _count(session, prefix, "jobs", where=f"WHERE id = {blocked.id}") == 1
    assert (
        _count(
            session,
            prefix,
            "ready_executions",
            where=f"WHERE job_id = {blocked.id}",
        )
        == 1
    )


def test_unblock_unknown_id_returns_false(qc) -> None:
    assert qc.unblock(999_999) is False


def test_cancel_blocked_removes_blocked_execution_and_job(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ExclusiveJob)
    _first, blocked = _enqueue_blocked_pair(qc, ExclusiveJob)
    session.expire_all()

    assert qc.cancel_blocked(blocked.id) is True

    session.expire_all()
    assert _count(session, prefix, "blocked_executions") == 0
    assert _count(session, prefix, "jobs", where=f"WHERE id = {blocked.id}") == 0


def test_cancel_blocked_unknown_id_returns_false(qc) -> None:
    assert qc.cancel_blocked(999_999) is False


def test_unblock_all_without_filters_promotes_everything(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ExclusiveJob)
    qc.register_job(OtherExclusiveJob)
    _enqueue_blocked_pair(qc, ExclusiveJob)
    _enqueue_blocked_pair(qc, OtherExclusiveJob)
    session.expire_all()
    assert _count(session, prefix, "blocked_executions") == 2

    count = qc.unblock_all()
    assert count == 2

    session.expire_all()
    assert _count(session, prefix, "blocked_executions") == 0
    # Each promoted blocked row produced a fresh ready_executions row.
    # The first job per concurrency key already had a ready row from the
    # initial enqueue, so the table now holds 2 (initial) + 2 (promoted) = 4.
    assert _count(session, prefix, "ready_executions") == 4


def test_unblock_all_filters_by_class_name(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ExclusiveJob)
    qc.register_job(OtherExclusiveJob)
    _enqueue_blocked_pair(qc, ExclusiveJob)
    _enqueue_blocked_pair(qc, OtherExclusiveJob)
    session.expire_all()

    count = qc.unblock_all(class_name=ExclusiveJob.__qualname__)
    assert count == 1

    session.expire_all()
    # OtherExclusiveJob's blocked row remains
    assert _count(session, prefix, "blocked_executions") == 1
    # The promoted ExclusiveJob row now sits in ready_executions alongside
    # the initial ready row for the first ExclusiveJob enqueue.
    assert (
        _count(
            session,
            prefix,
            "ready_executions",
            where="WHERE queue_name = 'default'",
        )
        == 3
    )


def test_unblock_all_filters_by_queue_name(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    class EmailExclusiveJob(quebec.ActiveJob):
        queue_as = "emails"
        concurrency_limit = 1

        @staticmethod
        def concurrency_key(*args, **kwargs) -> str:
            return "email-exclusive"

        def perform(self, *args, **kwargs):
            pass

    qc.register_job(ExclusiveJob)
    qc.register_job(EmailExclusiveJob)
    _enqueue_blocked_pair(qc, ExclusiveJob)
    _enqueue_blocked_pair(qc, EmailExclusiveJob)
    session.expire_all()
    assert _count(session, prefix, "blocked_executions") == 2

    count = qc.unblock_all(queue_name="emails")
    assert count == 1

    session.expire_all()
    # default-queue row remains
    assert (
        _count(
            session,
            prefix,
            "blocked_executions",
            where="WHERE queue_name = 'default'",
        )
        == 1
    )


class DelayableJob(quebec.BaseClass):
    queue_as = "default"

    def perform(self, *args, **kwargs):
        pass


def test_cancel_scheduled_marks_finished_and_removes_execution(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(DelayableJob)
    job = DelayableJob.set(wait=3600).perform_later(qc)
    session.expire_all()
    assert _count(session, prefix, "scheduled_executions") == 1

    assert qc.cancel_scheduled(job.id) is True

    session.expire_all()
    assert _count(session, prefix, "scheduled_executions") == 0
    finished_at = session.execute(
        text(f"SELECT finished_at FROM {prefix}_jobs WHERE id = :id"),
        {"id": job.id},
    ).scalar()
    assert finished_at is not None


def test_cancel_scheduled_unknown_id_returns_false(qc) -> None:
    assert qc.cancel_scheduled(999_999) is False


class RecurringDummyJob(quebec.ActiveJob):
    queue_as = "default"

    def perform(self, *args, **kwargs):
        pass


def _insert_recurring_task(session, prefix, key: str, class_name: str) -> None:
    """Insert a recurring_tasks row directly so we don't have to spin up the scheduler."""
    session.execute(
        text(
            f"INSERT INTO {prefix}_recurring_tasks "
            f"(key, schedule, class_name, queue_name, priority, static, "
            f"arguments, created_at, updated_at) VALUES "
            f"(:key, :schedule, :class_name, :queue_name, :priority, :static, "
            f":arguments, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        ),
        {
            "key": key,
            "schedule": "every minute",
            "class_name": class_name,
            "queue_name": "default",
            "priority": 0,
            "static": True,
            "arguments": "[]",
        },
    )
    session.commit()


def test_run_recurring_now_enqueues_job(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(RecurringDummyJob)
    _insert_recurring_task(
        session, prefix, key="daily-cleanup", class_name=RecurringDummyJob.__qualname__
    )

    assert qc.run_recurring_now("daily-cleanup") is True

    session.expire_all()
    # One job created plus one recurring_executions row recording the run.
    assert (
        _count(
            session,
            prefix,
            "jobs",
            where=f"WHERE class_name = '{RecurringDummyJob.__qualname__}'",
        )
        == 1
    )
    assert (
        _count(
            session,
            prefix,
            "recurring_executions",
            where="WHERE task_key = 'daily-cleanup'",
        )
        == 1
    )


def test_run_recurring_now_unknown_key_returns_false(qc) -> None:
    assert qc.run_recurring_now("does-not-exist") is False


def test_run_recurring_now_consecutive_calls_both_enqueue(qc_with_sqlalchemy) -> None:
    """Each manual run uses ``now()`` for ``scheduled_at`` so it gets its own
    recurring_executions row — back-to-back calls each enqueue a fresh job."""
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(RecurringDummyJob)
    _insert_recurring_task(
        session, prefix, key="manual-runs", class_name=RecurringDummyJob.__qualname__
    )

    assert qc.run_recurring_now("manual-runs") is True
    assert qc.run_recurring_now("manual-runs") is True

    session.expire_all()
    assert (
        _count(
            session,
            prefix,
            "recurring_executions",
            where="WHERE task_key = 'manual-runs'",
        )
        == 2
    )


def test_run_recurring_now_respects_concurrency_limit(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ExclusiveJob)
    blocker = ExclusiveJob.perform_later(qc)
    session.expire_all()
    assert _count(session, prefix, "ready_executions") == 1

    _insert_recurring_task(
        session, prefix, key="exclusive-recurring", class_name=ExclusiveJob.__qualname__
    )
    assert qc.run_recurring_now("exclusive-recurring") is True

    session.expire_all()
    assert _count(session, prefix, "recurring_executions") == 1
    assert _count(session, prefix, "ready_executions") == 1
    ready_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_ready_executions")
    ).scalar()
    assert ready_job_id == blocker.id

    assert _count(session, prefix, "blocked_executions") == 1
    blocked_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_blocked_executions")
    ).scalar()
    assert blocked_job_id != blocker.id

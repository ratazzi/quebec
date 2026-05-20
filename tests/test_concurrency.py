"""Tests for concurrency control: semaphore, blocked executions, unblock flow."""

from __future__ import annotations

import quebec
from sqlalchemy import text


class ConcurrentJob(quebec.BaseClass):
    concurrency_limit = 1
    calls: list[int] = []

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "exclusive-resource"

    def perform(self, value: int) -> None:
        type(self).calls.append(value)


def test_first_job_goes_to_ready_second_goes_to_blocked(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    ConcurrentJob.calls = []
    qc.register_job(ConcurrentJob)

    ConcurrentJob.perform_later(qc, 1)
    ConcurrentJob.perform_later(qc, 2)

    assert db_assert.count_jobs() == 2
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 1


def test_completing_first_job_releases_semaphore(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    ConcurrentJob.calls = []
    qc.register_job(ConcurrentJob)

    ConcurrentJob.perform_later(qc, 1)

    # Semaphore value=0 (acquired)
    sem = session.execute(text(f"SELECT value FROM {prefix}_semaphores")).fetchone()
    assert sem.value == 0

    # Execute first job
    execution = qc.drain_one()
    execution.perform()
    session.expire_all()

    assert ConcurrentJob.calls == [1]
    # Semaphore released (value back to 1)
    sem = session.execute(text(f"SELECT value FROM {prefix}_semaphores")).fetchone()
    assert sem.value == 1


def test_semaphore_created_on_first_concurrent_job(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ConcurrentJob)

    ConcurrentJob.perform_later(qc, 1)

    semaphores = session.execute(text(f"SELECT * FROM {prefix}_semaphores")).fetchall()
    assert len(semaphores) == 1
    assert semaphores[0].value == 0  # limit=1, one acquired → value=0


class DiscardOnConflictJob(quebec.BaseClass):
    concurrency_limit = 1
    concurrency_on_conflict = quebec.ConcurrencyConflict.Discard

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "singleton"

    def perform(self, value: int) -> None:
        return None


def test_discard_on_conflict_discards_second_job(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]

    qc.register_job(DiscardOnConflictJob)

    DiscardOnConflictJob.perform_later(qc, 1)
    DiscardOnConflictJob.perform_later(qc, 2)

    # First goes to ready, second is discarded (not blocked)
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 0
    # Discarded job should be finished (not failed)
    assert db_assert.count_failed_executions() == 0


def test_blocked_job_promoted_immediately_after_release_with_limit_one(
    qc_with_sqlalchemy, db_assert
) -> None:
    """Regression: with concurrency_limit=1, completing job A must release the
    semaphore in a way that lets the blocked job B be promoted to ready right
    away — NOT after ``concurrency_duration`` seconds.

    Previously ``acquire_semaphore`` short-circuited to ``false`` whenever the
    semaphore row existed with ``concurrency_limit == 1``, so the row had to
    be physically deleted by the dispatcher's expired-key sweep before any
    new acquire could succeed. That turned ``concurrency_duration`` into a
    minimum cooldown instead of a crash-safety TTL.
    """
    qc = qc_with_sqlalchemy["qc"]

    ConcurrentJob.calls = []
    qc.register_job(ConcurrentJob)

    # A → ready, B → blocked.
    ConcurrentJob.perform_later(qc, 1)
    ConcurrentJob.perform_later(qc, 2)
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 1

    # Drain A. Its release path (`release_next_blocked_job`) must promote B.
    execution_a = qc.drain_one()
    execution_a.perform()

    # B is now claimable — without waiting on concurrency_duration.
    assert db_assert.count_blocked_executions() == 0
    assert db_assert.count_ready_executions() == 1

    execution_b = qc.drain_one()
    execution_b.perform()
    assert ConcurrentJob.calls == [1, 2]


def test_discard_mode_can_reenqueue_after_release_with_limit_one(
    qc_with_sqlalchemy, db_assert
) -> None:
    """Regression: with concurrency_limit=1 + Discard mode, a chained
    ``perform_later`` issued right after the job finishes must land in ready,
    not be discarded. Previously the freed semaphore row was still readable
    by the ``limit == 1`` short-circuit in ``acquire_semaphore``, forcing
    every re-enqueue within ``concurrency_duration`` seconds to be discarded.
    """
    qc = qc_with_sqlalchemy["qc"]

    qc.register_job(DiscardOnConflictJob)

    DiscardOnConflictJob.perform_later(qc, 1)
    execution = qc.drain_one()
    execution.perform()

    # Job A is finished and the semaphore is released. A fresh perform_later
    # for the same concurrency_key must acquire and reach ready_executions.
    DiscardOnConflictJob.perform_later(qc, 2)
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 0
    # The new job is NOT marked finished (i.e. NOT discarded).
    assert db_assert.count_jobs() == 2

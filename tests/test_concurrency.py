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

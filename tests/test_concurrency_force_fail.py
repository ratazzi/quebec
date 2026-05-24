"""Regression: force-failing a dead worker's claimed execution must release the
class-level concurrency lock and promote the next blocked job.

Mirror of Solid Queue PR #547 / commit 95dc9b4: ``ClaimedExecution.fail_all_with``
used to only write failed_executions rows without releasing the class-level
concurrency semaphore, so jobs blocked on the same ``concurrency_key`` stayed
stuck until the semaphore TTL expired (default 60s).

Quebec routes every force-fail path (supervisor reap, orphan sweep,
``prune_dead_processes``) through ``Worker::fail_claimed_execution`` →
``unblock_next_job``, which releases the semaphore and promotes the next blocked
job. This test locks that behavior in via the supervisor force-fail entrypoint
``qc.supervisor_fail_claimed_by_process_id``.
"""

from __future__ import annotations

import quebec
from sqlalchemy import text


class ForceFailJob(quebec.BaseClass):
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "exclusive-resource"

    def perform(self, value: int) -> None:
        return None


def test_force_fail_releases_lock_and_promotes_blocked(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ForceFailJob)

    # Register a process row to act as the (soon to be dead) worker that holds
    # job A's claim.
    process_id = qc.register_supervisor()

    # A enqueues normally: acquires the only slot for the key, lands in ready.
    job_a = ForceFailJob.perform_later(qc, 1)
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 0

    # Simulate A having been claimed by that worker: move it from ready to
    # claimed under the process. The class-level semaphore stays held.
    session.execute(
        text(f"DELETE FROM {prefix}_ready_executions WHERE job_id = :jid"),
        {"jid": job_a.id},
    )
    session.execute(
        text(
            f"INSERT INTO {prefix}_claimed_executions "
            "(job_id, process_id, created_at) "
            "VALUES (:jid, :pid, CURRENT_TIMESTAMP)"
        ),
        {"jid": job_a.id, "pid": process_id},
    )
    session.commit()
    session.expire_all()
    assert db_assert.count_claimed_executions() == 1

    # B enqueues for the same key while A holds the slot -> blocked.
    job_b = ForceFailJob.perform_later(qc, 2)
    session.expire_all()
    assert db_assert.count_blocked_executions() == 1
    assert db_assert.count_ready_executions() == 0
    blocked_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_blocked_executions")
    ).scalar()
    assert blocked_job_id == job_b.id

    # Semaphore is held (value=0) before the worker dies.
    sem = session.execute(text(f"SELECT value FROM {prefix}_semaphores")).fetchone()
    assert sem.value == 0

    # The worker dies: supervisor force-fails its claimed executions. This is
    # the path that must also release the class-level lock and unblock B.
    failed = qc.supervisor_fail_claimed_by_process_id(process_id)
    assert failed == 1

    session.expire_all()

    # A's claim is gone and recorded as failed.
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.job_is_failed(job_a.id)

    # The lock was released: B is promoted from blocked to ready immediately,
    # not left waiting for the semaphore TTL to expire.
    assert db_assert.count_blocked_executions() == 0, (
        "blocked job must be promoted after the held lock is released"
    )
    assert db_assert.count_ready_executions() == 1
    ready_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_ready_executions")
    ).scalar()
    assert ready_job_id == job_b.id

    # The slot is now held by B (value back to 0 after its promotion acquire).
    sem = session.execute(text(f"SELECT value FROM {prefix}_semaphores")).fetchone()
    assert sem.value == 0

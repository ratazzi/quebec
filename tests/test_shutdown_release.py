# coding: utf-8
"""Regression tests for the graceful-shutdown claimed-execution release path.

During graceful shutdown a worker releases its claimed-but-not-started jobs
back to ready so the dispatcher can re-claim them. Jobs that are *currently
inside* ``perform()`` must NOT be released: ``graceful_shutdown`` ends with
``process::exit(0)``, killing the perform thread, so releasing the job would
let a neighbour worker claim and run the same job concurrently (duplicate
execution + lost result).

These tests drive ``release_claimed_for_shutdown`` (the same code path the
shutdown branch of ``run_main_loop`` calls) directly, without the full main
loop / SIGTERM dance, so the race is reproduced deterministically.
"""

from __future__ import annotations

import threading

import quebec
from sqlalchemy import text

from .helpers import wait_until


def _count(session, prefix: str, table: str) -> int:
    return session.execute(text(f"SELECT COUNT(*) FROM {prefix}_{table}")).scalar()


def test_in_flight_job_is_not_released_on_shutdown(qc_with_sqlalchemy):
    """A job inside perform() stays in claimed_executions on shutdown release."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    started = threading.Event()
    release_perform = threading.Event()

    class BlockingJob(quebec.ActiveJob):
        def perform(self, *args, **kwargs):
            started.set()
            # Hold inside perform() to keep the execution "in-flight" while the
            # shutdown release runs.
            release_perform.wait(timeout=10)
            return True

    qc.register_job(BlockingJob)

    # Register a process so claimed rows carry a known process_id; the shutdown
    # release filters by it.
    process_id = qc.register_worker_process()

    enqueued = BlockingJob.perform_later(qc)
    execution = qc.drain_one()

    session.expire_all()
    assert _count(session, prefix, "claimed_executions") == 1
    assert _count(session, prefix, "ready_executions") == 0

    perform_thread = threading.Thread(target=execution.perform, daemon=True)
    perform_thread.start()
    try:
        wait_until(
            started.is_set,
            timeout=5,
            message="perform() never started",
        )

        # Drive the graceful-shutdown release while the job is mid-perform.
        released = qc.release_claimed_for_shutdown(process_id)

        # The in-flight job must NOT be released: nothing returned to ready,
        # and the claimed row is still present.
        assert released == 0
        session.expire_all()
        assert _count(session, prefix, "ready_executions") == 0
        assert _count(session, prefix, "claimed_executions") == 1
    finally:
        release_perform.set()
        perform_thread.join(timeout=5)

    assert not perform_thread.is_alive()

    # The original perform completed normally via after_executed(): the claimed
    # row is gone, the job is finished, and it never bounced back to ready.
    session.expire_all()
    assert _count(session, prefix, "claimed_executions") == 0
    assert _count(session, prefix, "ready_executions") == 0
    finished = session.execute(
        text(f"SELECT finished_at FROM {prefix}_jobs WHERE id = :id"),
        {"id": enqueued.id},
    ).scalar()
    assert finished is not None


def test_claimed_but_not_started_job_is_released_on_shutdown(qc_with_sqlalchemy):
    """A claimed job that never entered perform() IS released back to ready."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    class IdleJob(quebec.ActiveJob):
        def perform(self, *args, **kwargs):
            return True

    qc.register_job(IdleJob)

    process_id = qc.register_worker_process()

    IdleJob.perform_later(qc)
    # Claim it via the real batch path so the worker-local ledger records it as
    # Dispatched, then do NOT perform it. Release is ledger-authoritative, so a
    # claimed-but-not-started job must carry its Dispatched entry to be released
    # (drain_one bypasses the ledger and no longer represents a production
    # claim). Keep the Execution alive so its Drop backstop doesn't clear the
    # entry before release runs.
    claimed = qc.drain_batch(1)
    assert len(claimed) == 1

    session.expire_all()
    assert _count(session, prefix, "claimed_executions") == 1
    assert _count(session, prefix, "ready_executions") == 0

    released = qc.release_claimed_for_shutdown(process_id)

    assert released == 1
    session.expire_all()
    assert _count(session, prefix, "claimed_executions") == 0
    assert _count(session, prefix, "ready_executions") == 1
    del claimed

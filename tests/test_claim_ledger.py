# coding: utf-8
"""Regression tests for the claim ledger that tracks a worker's owned claims.

The ledger maps ``claimed_executions.id`` to a lifecycle state:

* ``Dispatched`` (0) — claimed and queued, not yet performing.
* ``InFlight`` (1)   — inside ``perform()``.
* ``CleanupPending`` (2) — performed, but ``after_executed`` cleanup failed.

The batch-claim path (exercised here via ``drain_batch``) records each claim as
``Dispatched``; the single-claim ``drain_one`` test helper bypasses the ledger,
so these tests claim via ``drain_batch(1)`` to drive the real transition.

The shutdown release path (``release_claimed_for_shutdown``) only requeues
``Dispatched`` claims; ``InFlight`` and ``CleanupPending`` are skipped because
the job either is still running (kill-after-exit would duplicate it) or has
already executed. ``should_drain_exit`` self-exits only when the ledger is
empty. These tests pin those transitions deterministically.
"""

from __future__ import annotations

import quebec
from sqlalchemy import text


def _claimed_id(session, prefix: str, job_id: int) -> int:
    """Return the claimed_executions.id for a given job_id."""
    return session.execute(
        text(f"SELECT id FROM {prefix}_claimed_executions WHERE job_id = :jid"),
        {"jid": job_id},
    ).scalar()


class _IdleJob(quebec.ActiveJob):
    def perform(self, *args, **kwargs):
        return True


def test_claim_sets_dispatched(qc_with_sqlalchemy, db_assert):
    """Claiming a job records it as Dispatched in the ledger."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    qc.register_job(_IdleJob)
    qc.register_worker_process()

    enqueued = _IdleJob.perform_later(qc)
    # Hold the Execution: dropping it would clear its (non-CleanupPending)
    # ledger entry via Execution::Drop before we can observe it.
    (execution,) = qc.drain_batch(1)

    session.expire_all()
    assert db_assert.count_claimed_executions() == 1
    claimed_id = _claimed_id(session, prefix, enqueued.id)

    assert qc._ledger_state(claimed_id) == 0
    del execution


def test_perform_removes_from_ledger(qc_with_sqlalchemy, db_assert):
    """A successful perform() removes the claim from the ledger."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    qc.register_job(_IdleJob)
    qc.register_worker_process()

    enqueued = _IdleJob.perform_later(qc)
    (execution,) = qc.drain_batch(1)

    session.expire_all()
    claimed_id = _claimed_id(session, prefix, enqueued.id)
    assert qc._ledger_state(claimed_id) == 0

    execution.perform()

    assert qc._ledger_state(claimed_id) is None


def test_cleanup_pending_not_released_on_shutdown(qc_with_sqlalchemy, db_assert):
    """CleanupPending claims are skipped by the shutdown release."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    qc.register_job(_IdleJob)
    process_id = qc.register_worker_process()

    enqueued = _IdleJob.perform_later(qc)
    qc.drain_batch(1)

    session.expire_all()
    claimed_id = _claimed_id(session, prefix, enqueued.id)

    # Job already executed but cleanup failed: must NOT be requeued.
    qc._set_ledger_state(claimed_id, 2)

    released = qc.release_claimed_for_shutdown(process_id)

    assert released == 0
    session.expire_all()
    assert db_assert.count_claimed_executions() == 1
    assert db_assert.count_ready_executions() == 0


def test_dispatched_released_on_shutdown(qc_with_sqlalchemy, db_assert):
    """A Dispatched (never-picked) claim IS released back to ready."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    qc.register_job(_IdleJob)
    process_id = qc.register_worker_process()

    enqueued = _IdleJob.perform_later(qc)
    qc.drain_batch(1)

    session.expire_all()
    claimed_id = _claimed_id(session, prefix, enqueued.id)

    # Simulate a claim that was dispatched but never picked up by perform().
    qc._set_ledger_state(claimed_id, 0)

    released = qc.release_claimed_for_shutdown(process_id)

    assert released == 1
    session.expire_all()
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_ready_executions() == 1


def test_in_flight_not_released_on_shutdown(qc_with_sqlalchemy, db_assert):
    """An InFlight claim is skipped by the shutdown release."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    qc.register_job(_IdleJob)
    process_id = qc.register_worker_process()

    enqueued = _IdleJob.perform_later(qc)
    qc.drain_batch(1)

    session.expire_all()
    claimed_id = _claimed_id(session, prefix, enqueued.id)

    qc._set_ledger_state(claimed_id, 1)

    released = qc.release_claimed_for_shutdown(process_id)

    assert released == 0
    session.expire_all()
    assert db_assert.count_claimed_executions() == 1
    assert db_assert.count_ready_executions() == 0


def test_should_drain_exit_tracks_ledger_emptiness(db_url, test_prefix):
    """should_drain_exit is gated on the ledger being empty."""
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, quiet_then_exit=True)
    assert qc.create_tables() is True
    try:
        qc.register_worker_process()
        qc.quiet()
        # Empty ledger → exit requested.
        assert qc.should_drain_exit() is True
        # Any Dispatched entry makes the ledger non-empty → must not exit.
        qc._set_ledger_state(999, 0)
        assert qc.should_drain_exit() is False
    finally:
        qc.close()

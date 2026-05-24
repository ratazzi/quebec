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
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def _claimed_id(session, prefix: str, job_id: int) -> int:
    """Return the claimed_executions.id for a given job_id."""
    return session.execute(
        text(f"SELECT id FROM {prefix}_claimed_executions WHERE job_id = :jid"),
        {"jid": job_id},
    ).scalar()


def _seed_claimed(session, prefix: str, process_id: int) -> int:
    """Seed a job + claimed_executions row directly (no ledger entry).

    Mirrors the seeding in test_supervisor.py / test_supervisor_pruned_race.py:
    bypassing ``drain_batch`` means the worker-local ledger never learns about
    this claim, reproducing the crash-fallback case where the in-process ledger
    was lost. Returns the new claimed_executions.id.
    """
    now_sql = "CURRENT_TIMESTAMP"
    session.execute(
        text(
            f"INSERT INTO {prefix}_jobs "
            f"(queue_name, class_name, arguments, priority, active_job_id, "
            f"scheduled_at, finished_at, concurrency_key, created_at, updated_at) "
            f"VALUES ('default', 'CrashJob', '[]', 0, 'ajid', NULL, NULL, NULL, "
            f"{now_sql}, {now_sql})"
        )
    )
    job_id = session.execute(text("SELECT last_insert_rowid()")).scalar()
    session.execute(
        text(
            f"INSERT INTO {prefix}_claimed_executions "
            f"(job_id, process_id, created_at) "
            f"VALUES (:job_id, :process_id, {now_sql})"
        ),
        {"job_id": job_id, "process_id": process_id},
    )
    session.commit()
    return _claimed_id(session, prefix, job_id)


def _seed_orphan(session, prefix: str, *, queue_name: str = "default") -> int:
    """Seed a job + a process-less (orphaned) claimed_executions row.

    ``process_id`` is NULL, so the DB orphan-sweep (``find_orphaned``) reclaims
    it as a crashed-worker leftover. The job's ``queue_name`` drives whether the
    sweep's per-row ``unblock_next_job`` calls the EXPERIMENTAL
    ``release_queue_slot`` (only for queues in ``experimental_queue_concurrency``).
    Returns the new job_id.
    """
    now_sql = "CURRENT_TIMESTAMP"
    session.execute(
        text(
            f"INSERT INTO {prefix}_jobs "
            f"(queue_name, class_name, arguments, priority, active_job_id, "
            f"scheduled_at, finished_at, concurrency_key, created_at, updated_at) "
            f"VALUES (:queue, 'CrashJob', '[]', 0, 'ajid', NULL, NULL, NULL, "
            f"{now_sql}, {now_sql})"
        ),
        {"queue": queue_name},
    )
    job_id = session.execute(text("SELECT last_insert_rowid()")).scalar()
    session.execute(
        text(
            f"INSERT INTO {prefix}_claimed_executions "
            f"(job_id, process_id, created_at) "
            f"VALUES (:job_id, NULL, {now_sql})"
        ),
        {"job_id": job_id},
    )
    session.commit()
    return job_id


def _seed_stale_process(session, prefix: str) -> int:
    """Insert a Worker process row whose heartbeat is far in the past.

    ``find_prunable`` selects rows with ``last_heartbeat_at`` older than
    ``now - process_alive_threshold`` (default 300s), so a year-2000 heartbeat
    is reliably stale. Returns the new processes.id.
    """
    session.execute(
        text(
            f"INSERT INTO {prefix}_processes "
            f"(kind, last_heartbeat_at, pid, hostname, metadata, created_at, name) "
            f"VALUES ('Worker', '2000-01-01 00:00:00', 4242, 'stale-host', NULL, "
            f"'2000-01-01 00:00:00', 'Worker-4242')"
        )
    )
    process_id = session.execute(text("SELECT last_insert_rowid()")).scalar()
    session.commit()
    return process_id


def _seed_claimed_for_process(
    session, prefix: str, process_id: int, *, queue_name: str = "default"
) -> int:
    """Seed a job + a claimed_executions row owned by ``process_id``.

    The job's ``queue_name`` drives whether the prune path's per-row
    ``unblock_next_job`` calls the EXPERIMENTAL ``release_queue_slot`` (only for
    queues in ``experimental_queue_concurrency``). Returns the new job_id.
    """
    now_sql = "CURRENT_TIMESTAMP"
    session.execute(
        text(
            f"INSERT INTO {prefix}_jobs "
            f"(queue_name, class_name, arguments, priority, active_job_id, "
            f"scheduled_at, finished_at, concurrency_key, created_at, updated_at) "
            f"VALUES (:queue, 'CrashJob', '[]', 0, 'ajid', NULL, NULL, NULL, "
            f"{now_sql}, {now_sql})"
        ),
        {"queue": queue_name},
    )
    job_id = session.execute(text("SELECT last_insert_rowid()")).scalar()
    session.execute(
        text(
            f"INSERT INTO {prefix}_claimed_executions "
            f"(job_id, process_id, created_at) "
            f"VALUES (:job_id, :process_id, {now_sql})"
        ),
        {"job_id": job_id, "process_id": process_id},
    )
    session.commit()
    return job_id


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


def test_dispatch_reconcile_requeues_residual(qc_with_sqlalchemy, db_assert):
    """A residual whose dispatch-failure requeue also failed is requeued by the
    maintenance reconcile, its ledger entry cleared, and a re-run is a no-op."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    qc.register_job(_IdleJob)
    qc.register_worker_process()

    enqueued = _IdleJob.perform_later(qc)
    # Claim the job so a claimed_executions row exists for this process.
    qc.drain_batch(1)

    session.expire_all()
    claimed_id = _claimed_id(session, prefix, enqueued.id)
    assert db_assert.count_claimed_executions() == 1
    assert db_assert.count_ready_executions() == 0

    # Simulate a dispatch failure whose requeue also failed: the row is still
    # claimed + Dispatched in the ledger and stashed in the dispatch-retry set.
    qc._set_ledger_state(claimed_id, 0)
    qc._add_dispatch_retry(claimed_id)
    assert qc._ledger_state(claimed_id) == 0

    # The maintenance reconcile retries the requeue.
    qc._run_dispatch_reconcile_once()

    session.expire_all()
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_ready_executions() == 1
    assert qc._ledger_state(claimed_id) is None

    # A second reconcile is a no-op: the retry set is empty and nothing changes.
    qc._run_dispatch_reconcile_once()

    session.expire_all()
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_ready_executions() == 1
    assert qc._ledger_state(claimed_id) is None


def test_missing_ledger_entry_is_skipped_on_shutdown(qc_with_sqlalchemy, db_assert):
    """A claimed row with NO ledger entry is NOT requeued by graceful release.

    Release is ledger-authoritative: it requeues ONLY rows the ledger still
    records as ``Dispatched``. ``claim_jobs`` records every claim as
    ``Dispatched``, so a claimed row with no ledger entry is error /
    ``Execution::Drop`` residue, never a live Dispatched claim. Requeuing it is
    the duplicate-run hazard where an executed-but-cleanup-failed claim lost its
    ledger entry. So graceful release SKIPS it and leaves it claimed for the DB
    orphan-sweep, which reclaims it once ``on_stop`` deletes the process row.
    (The real crash path is handled entirely by ``fail_orphaned_executions``,
    not this graceful release.)
    """
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    process_id = qc.register_worker_process()

    # Seed the claim directly (no drain_batch), so the worker-local ledger has
    # no entry for it — the error/Drop-residue shape.
    claimed_id = _seed_claimed(session, prefix, process_id)
    assert qc._ledger_state(claimed_id) is None
    assert db_assert.count_claimed_executions() == 1
    assert db_assert.count_ready_executions() == 0

    released = qc.release_claimed_for_shutdown(process_id)

    # Not requeued: the row stays claimed (left for the orphan-sweep), nothing
    # goes back to ready.
    assert released == 0
    session.expire_all()
    assert db_assert.count_claimed_executions() == 1
    assert db_assert.count_ready_executions() == 0


def test_dispatched_release_clears_ledger_on_shutdown(qc_with_sqlalchemy, db_assert):
    """A successful shutdown release of a Dispatched claim clears its ledger entry.

    ``test_dispatched_released_on_shutdown`` pins the DB outcome (row requeued);
    this pins the ledger outcome so ``release_all_claimed_executions``' success
    arm does not leak the entry (close()/test-hook paths keep running, so a
    lingering entry would block ``should_drain_exit`` forever).
    """
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
    qc._set_ledger_state(claimed_id, 0)
    assert qc._ledger_state(claimed_id) == 0

    released = qc.release_claimed_for_shutdown(process_id)

    assert released == 1
    assert qc._ledger_state(claimed_id) is None


def test_delete_claimed_by_id_clears_ledger(qc_with_sqlalchemy, db_assert):
    """The job-record-missing dispatch path clears the ledger entry.

    ``process_available_jobs`` calls ``delete_claimed_by_id`` when a claimed
    row's job record is gone (FK would prevent a failed_executions insert). The
    claimed row is dropped, so its ledger entry must be cleared too, otherwise
    it leaks and blocks drain exit.
    """
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    process_id = qc.register_worker_process()
    claimed_id = _seed_claimed(session, prefix, process_id)

    # Stand in for the Dispatched ledger entry claim_jobs would have set.
    qc._set_ledger_state(claimed_id, 0)
    assert qc._ledger_state(claimed_id) == 0
    assert db_assert.count_claimed_executions() == 1

    qc._delete_claimed_by_id(claimed_id)

    session.expire_all()
    assert db_assert.count_claimed_executions() == 0
    assert qc._ledger_state(claimed_id) is None


def test_fail_claimed_by_id_clears_ledger(qc_with_sqlalchemy, db_assert):
    """The unregistered-runnable dispatch path clears the ledger entry.

    ``process_available_jobs`` calls ``fail_claimed_by_id`` when the job's
    runnable is not registered: it writes a failed_executions row, deletes the
    claimed row, and releases the semaphore in one transaction. The ledger
    entry must be cleared so the now-gone claim stops blocking drain exit.
    """
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    process_id = qc.register_worker_process()
    claimed_id = _seed_claimed(session, prefix, process_id)
    job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_claimed_executions WHERE id = :cid"),
        {"cid": claimed_id},
    ).scalar()

    qc._set_ledger_state(claimed_id, 0)
    assert qc._ledger_state(claimed_id) == 0
    assert db_assert.count_claimed_executions() == 1

    qc._fail_claimed_by_id(claimed_id, job_id, "Job handler not found: CrashJob")

    session.expire_all()
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_failed_executions() == 1
    assert qc._ledger_state(claimed_id) is None


def test_delete_claimed_by_id_clears_ledger_on_db_error(qc_with_sqlalchemy, db_assert):
    """The job-record-missing path clears the ledger even when the DB delete fails.

    If ``delete_claimed_by_id`` cannot delete the residual claimed row (DB
    hiccup), the row is correctly left for the DB orphan-sweep — but the
    worker-local ledger entry must still be dropped, otherwise it stays
    Dispatched forever and blocks ``should_drain_exit``. We force the failure
    deterministically by dropping the claimed_executions table out from under
    the Rust query.
    """
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    process_id = qc.register_worker_process()
    claimed_id = _seed_claimed(session, prefix, process_id)

    qc._set_ledger_state(claimed_id, 0)
    assert qc._ledger_state(claimed_id) == 0

    # Drop the table so the Rust delete_by_id errors → failure arm runs.
    session.execute(text(f"DROP TABLE {prefix}_claimed_executions"))
    session.commit()

    qc._delete_claimed_by_id(claimed_id)

    # The delete could not complete, but the ledger entry is cleared so it no
    # longer blocks drain; the residual is left for the orphan-sweep.
    assert qc._ledger_state(claimed_id) is None


def test_fail_claimed_by_id_clears_ledger_on_db_error(qc_with_sqlalchemy, db_assert):
    """The unregistered-runnable path clears the ledger even when the txn fails.

    If ``fail_claimed_by_id``'s transaction cannot complete (DB hiccup), the
    claimed row is left for the DB orphan-sweep — but the ledger entry must
    still be dropped so it stops blocking drain. We force the failure by
    dropping the failed_executions table so the in-transaction insert errors.
    """
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    process_id = qc.register_worker_process()
    claimed_id = _seed_claimed(session, prefix, process_id)
    job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_claimed_executions WHERE id = :cid"),
        {"cid": claimed_id},
    ).scalar()

    qc._set_ledger_state(claimed_id, 0)
    assert qc._ledger_state(claimed_id) == 0

    # Drop failed_executions so the transaction's insert errors → failure arm.
    session.execute(text(f"DROP TABLE {prefix}_failed_executions"))
    session.commit()

    qc._fail_claimed_by_id(claimed_id, job_id, "Job handler not found: CrashJob")

    # The transaction rolled back, but the ledger entry is cleared so it no
    # longer blocks drain; the residual is left for the orphan-sweep.
    assert qc._ledger_state(claimed_id) is None


def test_emergency_cleanup_verify_present_marks_cleanup_pending(
    qc_with_sqlalchemy, db_assert
):
    """after_executed's emergency tail marks CleanupPending when the row remains.

    The job already executed; the normal cleanup and fail_claimed both failed,
    but the fallback delete left the claimed row in place. The verification
    re-check confirms the row is still present, so the ledger entry must become
    CleanupPending: the shutdown release must NOT requeue an already-executed
    job, and the DB orphan-sweep reclaims the lingering row.
    """
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
    assert db_assert.count_claimed_executions() == 1

    # Row is still present → Ok(Some) arm → CleanupPending.
    execution._resolve_emergency_cleanup_ledger()

    assert qc._ledger_state(claimed_id) == 2
    del execution


def test_emergency_cleanup_verify_gone_removes_ledger(qc_with_sqlalchemy, db_assert):
    """after_executed's emergency tail removes the ledger entry when the row is gone.

    The fallback delete above succeeded: the verification re-check finds no row
    (Ok(None)), so the ledger entry must be cleared rather than left dangling.
    """
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

    # Simulate the fallback delete having removed the claimed row.
    session.execute(
        text(f"DELETE FROM {prefix}_claimed_executions WHERE id = :cid"),
        {"cid": claimed_id},
    )
    session.commit()
    assert db_assert.count_claimed_executions() == 0

    # Row confirmed gone → Ok(None) arm → ledger entry removed.
    execution._resolve_emergency_cleanup_ledger()

    assert qc._ledger_state(claimed_id) is None
    del execution


def test_emergency_cleanup_verify_error_removes_ledger(qc_with_sqlalchemy, db_assert):
    """after_executed's emergency tail removes the ledger entry when verify ERRORS.

    This is the F2 regression: if the verification find_by_id itself errors, the
    old code collapsed to CleanupPending and left a permanent zombie ledger entry
    (the fallback delete may have already removed the row, so nothing could ever
    reconcile it). The three-way match must instead remove the entry on Err and
    defer to the DB orphan-sweep. We force the verification query to error by
    dropping the claimed_executions table out from under the Rust query, exactly
    mirroring the delete/fail on_db_error tests.
    """
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

    # Drop the table so the Rust find_by_id verification errors → Err arm.
    session.execute(text(f"DROP TABLE {prefix}_claimed_executions"))
    session.commit()

    # Verification could not be performed → Err arm → ledger entry removed
    # (NOT left as a permanent CleanupPending zombie).
    execution._resolve_emergency_cleanup_ledger()

    assert qc._ledger_state(claimed_id) is None
    del execution


def test_in_flight_guard_marks_cleanup_pending_on_panic(db_url, test_prefix):
    """InFlightGuard's Drop marks the InFlight entry CleanupPending on unwind (F3).

    A Rust panic between ``InFlightGuard::new`` and the completion of
    ``after_executed`` skips the normal ledger transition. ``Execution::Drop``
    only covers this if the ``Execution`` is dropped during unwinding, but the
    Python runner may keep it alive, so the InFlight entry would leak forever and
    ``ledger_has_active`` would block the quiet_then_exit drain. ``InFlightGuard``
    is a stack local of ``invoke``, so its panic-only ``Drop`` reliably handles
    the entry on a panic unwind. It must NOT remove it: the job was inside
    ``perform()`` and may have committed side effects, and a removed entry leaves
    no ledger record — so if this process survives the panic and later gets a
    graceful shutdown, ``release_all_claimed_executions`` would treat the claimed
    row as releasable and requeue it (duplicate run). Instead it marks the entry
    ``CleanupPending``, which is skipped by the shutdown release, counts as
    non-active so it does not block drain, and leaves the DB row for the
    orphan-sweep. The hook builds a real guard against the live ledger and drops
    it inside a genuine ``catch_unwind`` panic.
    """
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, quiet_then_exit=True)
    assert qc.create_tables() is True
    try:
        qc.register_worker_process()
        # Guard inserts id=777 as InFlight, then a real panic unwinds through
        # its Drop, which must mark the entry CleanupPending (not remove it).
        qc._drop_in_flight_guard(777, True)
        assert qc._ledger_state(777) == 2
    finally:
        qc.close()


def test_in_flight_guard_normal_drop_keeps_entry(db_url, test_prefix):
    """InFlightGuard's Drop is a no-op on a normal (non-panicking) drop.

    On the normal path ``after_executed`` owns the transition (remove on cleanup
    success, CleanupPending on failure); a removal in ``Drop`` would wipe a
    CleanupPending mark since the guard drops right after ``after_executed``
    returns. So a normal drop must leave the InFlight entry exactly as the guard
    inserted it.
    """
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, quiet_then_exit=True)
    assert qc.create_tables() is True
    try:
        qc.register_worker_process()
        # Guard inserts id=778 as InFlight, then drops normally → entry stays.
        qc._drop_in_flight_guard(778, False)
        assert qc._ledger_state(778) == 1
    finally:
        qc.close()


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


def test_should_drain_exit_ignores_cleanup_pending(db_url, test_prefix):
    """A ledger holding ONLY CleanupPending entries is still drainable.

    CleanupPending means the job already executed successfully and only the DB
    orphan-sweep is left to reclaim the row; the worker has no active work, so
    quiet_then_exit must be allowed to exit rather than hang forever waiting for
    cleanup that may never succeed. A Dispatched or InFlight entry, by contrast,
    is real pending/active work and must block the exit.
    """
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, quiet_then_exit=True)
    assert qc.create_tables() is True
    try:
        qc.register_worker_process()
        qc.quiet()
        # Only CleanupPending in the ledger → no active work → drainable.
        qc._set_ledger_state(901, 2)
        assert qc._ledger_state(901) == 2
        assert qc.should_drain_exit() is True
        # Add a Dispatched entry → active work → must not exit.
        qc._set_ledger_state(902, 0)
        assert qc.should_drain_exit() is False
        # Flip it to InFlight → still active work → must not exit.
        qc._set_ledger_state(902, 1)
        assert qc.should_drain_exit() is False
    finally:
        qc.close()


def test_should_drain_exit_with_lingering_claimed_row(db_url, test_prefix):
    """Drain-exit is ledger-authoritative even when a claimed row lingers in DB.

    Regression for the quiet_then_exit hang: a CleanupPending ledger entry is
    created precisely when the job executed but its claimed-row cleanup failed,
    so the claimed_executions row is intentionally left in the DB for the
    orphan-sweep to reclaim. The sweep can only reclaim it once this process row
    is gone, so a DB-count gate (the old ``count_by_process_id`` query) would see
    the lingering row, report non-empty, and hang the drain-exit forever. The
    ledger says no active work, so should_drain_exit must return True.
    """
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, quiet_then_exit=True)
    assert qc.create_tables() is True

    sa_url = db_url.split("?")[0] if db_url.startswith("sqlite:") else db_url
    engine = create_engine(sa_url)
    session = sessionmaker(bind=engine)()
    try:
        process_id = qc.register_worker_process()
        qc.quiet()

        # Seed a real claimed row owned by THIS worker's process, then mark its
        # ledger entry CleanupPending — the exact shape of a failed-cleanup job.
        claimed_id = _seed_claimed(session, test_prefix, process_id)
        qc._set_ledger_state(claimed_id, 2)
        assert qc._ledger_state(claimed_id) == 2

        # Confirm the lingering row is really present and owned by this process.
        count = session.execute(
            text(f"SELECT COUNT(*) FROM {test_prefix}_claimed_executions"),
        ).scalar()
        assert count == 1

        # Old behaviour: count_by_process_id == 1 → False (hang). Now: ledger has
        # no active work → drainable.
        assert qc.should_drain_exit() is True
    finally:
        qc.close()
        session.close()
        engine.dispose()


def test_orphan_sweep_reclaims_remaining_rows_when_one_row_fails(db_url, test_prefix):
    """The DB orphan-sweep is per-row best-effort (F4 regression).

    The crash-path safety net (``fail_orphaned_executions`` in the worker, and
    the equivalent loop in ``Supervisor::run_maintenance``) must keep reclaiming
    the remaining orphaned ``claimed_executions`` rows even when one row's
    cleanup raises. A sibling fix made the EXPERIMENTAL ``release_queue_slot``
    propagate semaphore-release errors with ``?``; the orphan loop used ``?`` on
    the per-row ``fail_claimed_execution`` call, so a single failing row aborted
    the whole sweep and left every OTHER orphan un-reclaimed — weakening the
    last line of defense. After the fix the failing row is logged and skipped
    while the others are still reclaimed.

    Deterministic single-row failure of exactly the guarded class (a queue-slot
    release error inside the per-row transaction): with
    ``experimental_queue_concurrency={"locked": 1}`` set, an orphan on the
    "locked" queue makes the sweep's ``unblock_next_job`` call
    ``release_queue_slot`` → ``release_semaphore``; dropping the ``semaphores``
    table out from under the Rust query makes that UPDATE error and rolls back
    that row. An orphan on the unthrottled "free" queue skips
    ``release_queue_slot`` entirely and is reclaimed cleanly. This mirrors the
    table-drop injection idiom used by the emergency-cleanup/on-db-error tests
    above; no production injection flag is added.
    """
    qc = quebec.Quebec(
        db_url,
        table_name_prefix=test_prefix,
        experimental_queue_concurrency={"locked": 1},
    )
    assert qc.create_tables() is True

    sa_url = db_url.split("?")[0] if db_url.startswith("sqlite:") else db_url
    engine = create_engine(sa_url)
    session = sessionmaker(bind=engine)()
    try:
        bad_job = _seed_orphan(session, test_prefix, queue_name="locked")
        good_job = _seed_orphan(session, test_prefix, queue_name="free")
        assert (
            session.execute(
                text(f"SELECT COUNT(*) FROM {test_prefix}_claimed_executions")
            ).scalar()
            == 2
        )

        # Drop the semaphores table so the "locked"-queue orphan's
        # release_queue_slot errors, rolling back its per-row transaction; the
        # "free"-queue orphan never touches the experimental queue slot.
        session.execute(text(f"DROP TABLE {test_prefix}_semaphores"))
        session.commit()

        # Global orphan-sweep. The bad row aborts its own transaction; the sweep
        # must NOT propagate — it returns Ok and reports only the
        # successfully-reclaimed count.
        pruned, orphaned = qc.supervisor_run_maintenance(None)
        assert orphaned == 1, (
            "the sweep must reclaim the free-queue orphan even though the "
            "locked-queue orphan's cleanup failed; per-row best-effort, not "
            "abort-on-first-error"
        )

        session.expire_all()
        # Good orphan: claimed row gone, failure recorded so it can be retried.
        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {test_prefix}_claimed_executions "
                    f"WHERE job_id = :j"
                ),
                {"j": good_job},
            ).scalar()
            == 0
        )
        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {test_prefix}_failed_executions "
                    f"WHERE job_id = :j"
                ),
                {"j": good_job},
            ).scalar()
            == 1
        )
        # Bad orphan: its transaction rolled back, so the claimed row is still
        # there, left for the next sweep tick / another process to retry.
        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {test_prefix}_claimed_executions "
                    f"WHERE job_id = :j"
                ),
                {"j": bad_job},
            ).scalar()
            == 1
        )
    finally:
        session.close()
        engine.dispose()
        qc.close()


def test_stale_process_pruning_is_per_row_best_effort(db_url, test_prefix):
    """Stale-process pruning is per-row AND per-process best-effort (Finding C).

    ``prune_dead_processes`` (worker) and ``fail_claimed_by_process_id_inner``
    (supervisor, driven here via ``supervisor_run_maintenance``) used to fail a
    stale process's claimed rows + prune the process row inside a SINGLE
    transaction with ``?`` on each row. One row's cleanup error (e.g. an
    experimental queue-slot release error) rolled back the WHOLE transaction:
    none of the rows were failed AND the process row was NOT pruned. Because the
    orphan-sweep only reclaims rows whose process row is gone, that un-pruned
    process wedged crash-recovery for itself and every later stale process.

    The fix runs each row's cleanup in its own small transaction and prunes the
    process row regardless of partial row failure — handing the leftovers to the
    orphan-sweep (which now reclaims them once the process row is gone). This
    test pins both halves: the process is STILL pruned and the good row is
    failed, while the bad row is left for the orphan-sweep.

    Same deterministic injection idiom as the F4 orphan-sweep test: with
    ``experimental_queue_concurrency={"locked": 1}`` a row on the "locked" queue
    makes the per-row ``unblock_next_job`` call ``release_queue_slot`` →
    ``release_semaphore``; dropping the ``semaphores`` table makes that UPDATE
    error. The "free"-queue row skips ``release_queue_slot`` and is reclaimed.
    """
    qc = quebec.Quebec(
        db_url,
        table_name_prefix=test_prefix,
        experimental_queue_concurrency={"locked": 1},
    )
    assert qc.create_tables() is True

    sa_url = db_url.split("?")[0] if db_url.startswith("sqlite:") else db_url
    engine = create_engine(sa_url)
    session = sessionmaker(bind=engine)()
    try:
        process_id = _seed_stale_process(session, test_prefix)
        bad_job = _seed_claimed_for_process(
            session, test_prefix, process_id, queue_name="locked"
        )
        good_job = _seed_claimed_for_process(
            session, test_prefix, process_id, queue_name="free"
        )
        assert (
            session.execute(
                text(f"SELECT COUNT(*) FROM {test_prefix}_claimed_executions")
            ).scalar()
            == 2
        )

        # Drop the semaphores table so the "locked"-queue row's
        # release_queue_slot errors, rolling back only that row's transaction.
        session.execute(text(f"DROP TABLE {test_prefix}_semaphores"))
        session.commit()

        # Prune stale processes (no excluded id). One bad row must NOT abort the
        # whole process cleanup: the process is still pruned and the good row is
        # failed; the call must NOT raise.
        pruned, orphaned = qc.supervisor_run_maintenance(None)
        assert pruned == 1, "the stale process must still be pruned despite a bad row"

        session.expire_all()
        # The stale process row is gone — this is what lets the orphan-sweep
        # reclaim the leftover bad row on a later tick.
        assert (
            session.execute(
                text(f"SELECT COUNT(*) FROM {test_prefix}_processes WHERE id = :p"),
                {"p": process_id},
            ).scalar()
            == 0
        )
        # Good row: failed + claimed row removed.
        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {test_prefix}_claimed_executions "
                    f"WHERE job_id = :j"
                ),
                {"j": good_job},
            ).scalar()
            == 0
        )
        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {test_prefix}_failed_executions "
                    f"WHERE job_id = :j"
                ),
                {"j": good_job},
            ).scalar()
            == 1
        )
        # Bad row: its transaction rolled back, so the claimed row is still there,
        # now orphaned (process row gone) and left for the orphan-sweep.
        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {test_prefix}_claimed_executions "
                    f"WHERE job_id = :j"
                ),
                {"j": bad_job},
            ).scalar()
            == 1
        )
    finally:
        session.close()
        engine.dispose()
        qc.close()

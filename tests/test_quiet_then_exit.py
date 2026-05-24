"""Tests for the `quiet_then_exit` option.

When enabled, a quiet signal (SIGUSR1/SIGTSTP) makes the process exit on its own
once all of this worker's in-flight and claimed jobs have drained — with no
timeout — instead of waiting for a later SIGTERM. Mirrors Sidekiq Enterprise's
USR2 rolling restart. `should_drain_exit()` is the predicate the Python wait
loop polls to decide when to self-shut-down.
"""

from __future__ import annotations

import quebec


def test_should_drain_exit_disabled_by_default(qc_with_sqlalchemy):
    """Without quiet_then_exit, a quiet idle worker never self-exits."""
    qc = qc_with_sqlalchemy["qc"]
    qc.register_worker_process()
    qc.quiet()
    assert qc.should_drain_exit() is False


def test_should_drain_exit_true_when_drained(db_url, test_prefix):
    """quiet_then_exit + quiet + nothing in-flight/claimed → exit requested."""
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, quiet_then_exit=True)
    assert qc.create_tables() is True
    try:
        qc.register_worker_process()
        # Enabled but not quiet yet → no exit.
        assert qc.should_drain_exit() is False
        qc.quiet()
        # Quiet and fully drained → exit requested.
        assert qc.should_drain_exit() is True
    finally:
        qc.close()


def test_should_drain_exit_false_with_claimed_job(db_url, test_prefix):
    """A claimed-but-not-yet-run job (Dispatched in the ledger) blocks self-exit.

    Drain-exit is ledger-authoritative, so the blocking signal is a Dispatched
    ledger entry, not a DB row. ``drain_batch`` claims via the real batch path
    that records the entry (``drain_one`` bypasses the ledger and would no longer
    block — that DB-only residue is the orphan-sweep's job, not a drain blocker).
    """
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, quiet_then_exit=True)
    assert qc.create_tables() is True

    class IdleJob(quebec.ActiveJob):
        def perform(self, *args, **kwargs):
            return True

    try:
        qc.register_job(IdleJob)
        qc.register_worker_process()
        qc.quiet()
        IdleJob.perform_later(qc)
        (execution,) = qc.drain_batch(1)  # Dispatched in the ledger, not performed
        # Still has a claimed job → must not self-exit.
        assert qc.should_drain_exit() is False
        del execution
    finally:
        qc.close()


def test_should_drain_exit_disabled_when_supervised(db_url, test_prefix):
    """Standalone-only: under the fork supervisor (supervisor_pid set), a quiet
    drained worker does not self-exit — it would just be reforked.
    """
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, quiet_then_exit=True)
    assert qc.create_tables() is True
    try:
        qc.register_worker_process()
        # Records supervisor_pid from the current parent (non-zero), marking this
        # as a fork-supervised child.
        qc.watch_parent_pid()
        qc.quiet()
        # Drained, but supervised → must not self-exit.
        assert qc.should_drain_exit() is False
    finally:
        qc.close()


def test_should_drain_exit_blocked_by_claim_in_progress(db_url, test_prefix):
    """A claim transaction in progress blocks self-exit even when otherwise idle.

    Guards the race where a claim that passed the quiet gate is still publishing
    work: should_drain_exit must wait for it rather than exit mid-claim.
    """
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, quiet_then_exit=True)
    assert qc.create_tables() is True
    try:
        qc.register_worker_process()
        qc.quiet()
        # Idle → would exit.
        assert qc.should_drain_exit() is True
        # A claim in progress must block it.
        qc._set_claim_in_progress(True)
        assert qc.should_drain_exit() is False
        # Once the claim finishes, exit is allowed again.
        qc._set_claim_in_progress(False)
        assert qc.should_drain_exit() is True
    finally:
        qc.close()

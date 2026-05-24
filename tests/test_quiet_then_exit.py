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
    """A claimed-but-not-yet-run job blocks self-exit until it drains."""
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
        qc.drain_one()  # claimed, not performed
        # Still has a claimed job → must not self-exit.
        assert qc.should_drain_exit() is False
    finally:
        qc.close()

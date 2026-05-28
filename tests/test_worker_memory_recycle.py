"""Tests for worker RSS soft-limit recycle state."""

from __future__ import annotations

import quebec


def test_memory_recycle_exits_when_drained_under_supervisor(db_url, test_prefix):
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    assert qc.create_tables() is True
    try:
        qc.register_worker_process()
        qc.watch_parent_pid()

        qc._request_worker_memory_recycle(256 * 1024 * 1024)

        assert qc.is_quiet is True
        assert qc.is_worker_memory_recycling is True
        assert qc._should_worker_memory_recycle_exit() is True
        # Ordinary quiet_then_exit remains disabled under supervisor mode.
        assert qc.should_drain_exit() is False
    finally:
        qc.close()


def test_memory_recycle_waits_for_claimed_job_before_timeout(db_url, test_prefix):
    qc = quebec.Quebec(
        db_url,
        table_name_prefix=test_prefix,
        worker_memory_graceful_timeout=60,
    )
    assert qc.create_tables() is True

    class IdleJob(quebec.ActiveJob):
        def perform(self, *args, **kwargs):
            return True

    try:
        qc.register_job(IdleJob)
        qc.register_worker_process()
        qc._request_worker_memory_recycle(256 * 1024 * 1024)
        IdleJob.perform_later(qc)
        qc.drain_one()

        assert qc._should_worker_memory_recycle_exit() is False
    finally:
        qc.close()


def test_memory_recycle_timeout_allows_exit_with_claimed_job(db_url, test_prefix):
    qc = quebec.Quebec(
        db_url,
        table_name_prefix=test_prefix,
        worker_memory_graceful_timeout=0,
    )
    assert qc.create_tables() is True

    class IdleJob(quebec.ActiveJob):
        def perform(self, *args, **kwargs):
            return True

    try:
        qc.register_job(IdleJob)
        qc.register_worker_process()
        qc._request_worker_memory_recycle(256 * 1024 * 1024)
        IdleJob.perform_later(qc)
        qc.drain_one()

        assert qc._should_worker_memory_recycle_exit() is True
    finally:
        qc.close()

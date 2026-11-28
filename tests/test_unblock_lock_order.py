"""A finishing worker must not lock a blocked row while it holds the semaphore.

`qc.unblock()` and the dispatcher lock the blocked row first and then the
semaphore (Solid Queue's `BlockedExecution#release`). A finishing worker gave
its slot back and locked the next blocked row in one transaction, the
opposite order. With `use_skip_locked = False` it waited for a blocked row
that an unblock held instead of skipping it; when the unblock then reached
the semaphore, the two transactions deadlocked and the database rolled one
back, so either the unblock failed or the slot stayed taken until the
semaphore expired.

Like Solid Queue (`Job#unblock_next_blocked_job`), the worker now gives the
slot back in a transaction of its own and only then releases the next blocked
job, locking the blocked row before the semaphore like everyone else.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import text

import quebec

from .helpers import create_database_engine, wait_until


class SharedResourceJob(quebec.BaseClass):
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "shared-resource"

    def perform(self, *args, **kwargs) -> None:
        pass


def _scalar(engine, sql: str):
    with engine.connect() as connection:
        return connection.execute(text(sql)).scalar()


def _lock_waiters(engine) -> int:
    """Transactions in this database currently waiting for a row lock."""
    if engine.dialect.name == "postgresql":
        sql = (
            "SELECT COUNT(*) FROM pg_stat_activity "
            "WHERE datname = current_database() AND wait_event_type = 'Lock'"
        )
    else:
        sql = "SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'"
    return _scalar(engine, sql)


def test_finishing_job_commits_while_unblock_holds_the_blocked_row(
    db_url, test_prefix
) -> None:
    if db_url.startswith("sqlite:"):
        pytest.skip("SQLite has no row locks")
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, use_skip_locked=False)
    assert qc.create_tables() is True
    engine = create_database_engine(db_url)
    prefix = test_prefix
    key = engine.dialect.identifier_preparer.quote("key")
    try:
        qc.register_job(SharedResourceJob)
        holder = SharedResourceJob.perform_later(qc, "holder")
        waiting = SharedResourceJob.perform_later(qc, "waiting")
        assert _scalar(
            engine,
            f"SELECT COUNT(*) FROM {prefix}_blocked_executions WHERE job_id = {waiting.id}",
        ) == 1
        execution = qc.drain_one()

        # Stand in for qc.unblock(): lock the blocked row, then (below) the
        # semaphore, in one transaction.
        with engine.connect() as unblock, ThreadPoolExecutor(max_workers=1) as pool:
            transaction = unblock.begin()
            try:
                concurrency_key = unblock.execute(
                    text(
                        f"SELECT concurrency_key FROM {prefix}_blocked_executions "
                        f"WHERE job_id = {waiting.id} FOR UPDATE"
                    )
                ).scalar_one()
                finished = pool.submit(execution.perform)

                # The holder's finish commits even though the next blocked
                # row is locked...
                wait_until(
                    lambda: _scalar(
                        engine,
                        f"SELECT COUNT(*) FROM {prefix}_jobs "
                        f"WHERE id = {holder.id} AND finished_at IS NOT NULL",
                    )
                    == 1,
                    timeout=10,
                )
                # ...and the worker then waits for that blocked row. Only
                # touch the semaphore once it does: had the worker still held
                # the semaphore row while waiting, this would deadlock.
                wait_until(lambda: _lock_waiters(engine) >= 1, timeout=10)
                touched = unblock.execute(
                    text(
                        f"UPDATE {prefix}_semaphores SET value = value "
                        f"WHERE {key} = :key"
                    ),
                    {"key": concurrency_key},
                )
                assert touched.rowcount == 1
            finally:
                transaction.rollback()
            finished.result(timeout=10)

        # Once the lock is gone the worker releases the waiting job.
        wait_until(
            lambda: _scalar(
                engine,
                f"SELECT COUNT(*) FROM {prefix}_ready_executions WHERE job_id = {waiting.id}",
            )
            == 1,
            timeout=10,
        )
    finally:
        qc.close()
        engine.dispose()

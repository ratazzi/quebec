"""Live MySQL shared-key completion promotes blocked jobs exactly once."""

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote, urlsplit

import pytest

import quebec


class MySqlSharedCompletion(quebec.BaseClass):
    concurrency_limit = 8

    @staticmethod
    def concurrency_key(value):
        return "shared"

    def perform(self, value):
        return None


class MySqlLostAckMember(quebec.BaseClass):
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(value):
        return "lost-ack"

    def perform(self, value):
        return None


class MySqlLostAckCallback(quebec.BaseClass):
    def perform(self):
        return None


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_concurrent_completion_promotes_all_blocked_jobs():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    if parsed.scheme != "mysql":
        raise ValueError("TEST_MYSQL_URL must use mysql://")
    prefix = f"mc_{uuid.uuid4().hex[:10]}"
    qc = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )

    def count(table, condition=""):
        with observer.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {prefix}_{table} {condition}")
            return cursor.fetchone()[0]

    try:
        qc.create_tables()
        qc.register_job(MySqlSharedCompletion)
        qc.register_worker_process()
        qc.perform_all_later(
            [MySqlSharedCompletion.build(value) for value in range(16)]
        )
        assert count("ready_executions") == 8
        assert count("blocked_executions") == 8
        executions = qc.drain_batch(8)
        assert len(executions) == 8
        barrier = threading.Barrier(4)

        def complete(execution):
            barrier.wait(timeout=10)
            execution.perform()

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(complete, executions))
        assert count("jobs", "WHERE finished_at IS NOT NULL") == 8
        assert count("ready_executions") == 8
        assert count("blocked_executions") == 0
        assert count("claimed_executions") == 0
        assert count("failed_executions") == 0

        promoted = qc.drain_batch(8)
        assert len(promoted) == 8
        for execution in promoted:
            execution.perform()
        assert count("jobs", "WHERE finished_at IS NOT NULL") == 16
        assert count("ready_executions") == 0
        assert count("claimed_executions") == 0
        with observer.cursor() as cursor:
            cursor.execute(f"SELECT value FROM {prefix}_semaphores")
            assert cursor.fetchone() == (8,)
    finally:
        observer.close()
        qc.close()


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_lost_commit_ack_does_not_repeat_batch_or_semaphore_release():
    """A second cleanup after committed state must not replay batch/slot effects."""
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    if parsed.scheme != "mysql":
        raise ValueError("TEST_MYSQL_URL must use mysql://")
    prefix = f"mlack_{uuid.uuid4().hex[:10]}"
    qc = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )

    def one(sql, values=None):
        with observer.cursor() as cursor:
            cursor.execute(sql, values)
            return cursor.fetchone()[0]

    try:
        assert qc.create_tables() is True
        qc.register_job(MySqlLostAckMember)
        qc.register_job(MySqlLostAckCallback)
        qc.register_worker_process()
        with qc.batch(on_finish=MySqlLostAckCallback) as batch:
            job = MySqlLostAckMember.perform_later(qc, "one")
        (execution,) = qc.drain_batch(1)
        claimed_id = one(
            f"SELECT id FROM {prefix}_claimed_executions WHERE job_id=%s",
            (job.id,),
        )

        execution.perform()
        assert batch.reload().finished
        assert batch.completed_jobs == 1
        assert one(f"SELECT count(*) FROM {prefix}_batch_executions") == 0
        assert one(f"SELECT count(*) FROM {prefix}_ready_executions") == 1
        assert one(f"SELECT value FROM {prefix}_semaphores") == 1

        # The transaction committed, but the caller enters its retry path as
        # if the COMMIT response was lost. The absent claimed row owns the truth.
        qc._set_ledger_state(claimed_id, 1)
        execution.post(None, "")

        assert qc._ledger_state(claimed_id) is None
        assert batch.reload().completed_jobs == 1
        assert one(f"SELECT count(*) FROM {prefix}_jobs") == 2
        assert one(f"SELECT count(*) FROM {prefix}_ready_executions") == 1
        assert one(f"SELECT count(*) FROM {prefix}_batch_executions") == 0
        assert one(f"SELECT count(*) FROM {prefix}_claimed_executions") == 0
        assert one(f"SELECT count(*) FROM {prefix}_failed_executions") == 0
        assert one(f"SELECT value FROM {prefix}_semaphores") == 1

        (callback,) = qc.drain_batch(1)
        callback.perform()
        assert (
            one(f"SELECT count(*) FROM {prefix}_jobs WHERE finished_at IS NOT NULL")
            == 2
        )
        assert one(f"SELECT count(*) FROM {prefix}_ready_executions") == 0
    finally:
        observer.close()
        qc.close()

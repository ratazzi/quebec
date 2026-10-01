"""Live MySQL regression for concurrent bulk enqueue on one semaphore key."""

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote, urlsplit

import pytest

import quebec


class SharedKeyJob(quebec.BaseClass):
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(value):
        return "shared"

    def perform(self, value):
        return None


class DiscardSharedKeyJob(SharedKeyJob):
    concurrency_on_conflict = quebec.ConcurrencyConflict.Discard


class PlainJob(quebec.BaseClass):
    def perform(self, value):
        return None


@pytest.mark.skipif(not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL")
def test_bulk_batch_mixes_ready_scheduled_blocked_and_discard():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    prefix = f"mb_{uuid.uuid4().hex[:10]}"
    qc = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )
    try:
        qc.create_tables()
        for job_class in (PlainJob, SharedKeyJob, DiscardSharedKeyJob):
            qc.register_job(job_class)
        with qc.batch() as batch:
            qc.perform_all_later(
                [PlainJob.build(value) for value in range(4)]
                + [PlainJob.set(wait=3600).build(value) for value in range(4, 8)]
                + [SharedKeyJob.build(value) for value in range(8, 12)]
                + [DiscardSharedKeyJob.build(value) for value in range(12, 16)]
            )
        assert batch.reload().total_jobs == 16
        with observer.cursor() as cursor:
            for table, expected in (
                ("jobs", 16),
                ("ready_executions", 6),
                ("scheduled_executions", 4),
                ("blocked_executions", 3),
                ("batch_executions", 13),
                ("failed_executions", 0),
            ):
                cursor.execute(f"SELECT count(*) FROM {prefix}_{table}")
                assert cursor.fetchone()[0] == expected
            cursor.execute(f"SELECT count(*) FROM {prefix}_jobs WHERE finished_at IS NOT NULL")
            assert cursor.fetchone()[0] == 3
            cursor.execute(f"SELECT count(*), min(value), max(value) FROM {prefix}_semaphores")
            assert cursor.fetchone() == (2, 0, 0)
    finally:
        observer.close()
        qc.close()


@pytest.mark.skipif(not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL")
def test_four_producers_enqueue_one_shared_key_without_deadlock():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    if parsed.scheme != "mysql":
        raise ValueError("TEST_MYSQL_URL must use mysql://")
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )
    try:
        for _ in range(3):
            prefix = f"mt_{uuid.uuid4().hex[:10]}"
            instances = [
                quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
                for _ in range(4)
            ]
            try:
                instances[0].create_tables()
                for instance in instances:
                    instance.register_job(SharedKeyJob)
                barrier = threading.Barrier(4)

                def enqueue(index):
                    jobs = [SharedKeyJob.build(value) for value in range(index * 16, (index + 1) * 16)]
                    barrier.wait(timeout=10)
                    instances[index].perform_all_later(jobs)

                with ThreadPoolExecutor(max_workers=4) as pool:
                    list(pool.map(enqueue, range(4)))
                with observer.cursor() as cursor:
                    for table, expected in (
                        ("jobs", 64),
                        ("ready_executions", 1),
                        ("blocked_executions", 63),
                        ("failed_executions", 0),
                    ):
                        cursor.execute(f"SELECT count(*) FROM {prefix}_{table}")
                        assert cursor.fetchone()[0] == expected
                    cursor.execute(f"SELECT count(*), min(value), max(value) FROM {prefix}_semaphores")
                    assert cursor.fetchone() == (1, 0, 0)
            finally:
                for instance in instances:
                    instance.close()
    finally:
        observer.close()

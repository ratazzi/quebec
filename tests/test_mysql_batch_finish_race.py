"""A MySQL batch cannot finish ahead of a concurrent committed append."""

import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote, urlsplit

import pytest
import quebec


class RaceMember(quebec.BaseClass):
    def perform(self, value=None):
        return None


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_append_commit_before_final_completion_keeps_batch_open():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    prefix = f"mbfr_{uuid.uuid4().hex[:10]}"
    owner = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
    adder = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
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
        owner.create_tables()
        owner.register_job(RaceMember)
        adder.register_job(RaceMember)
        owner.register_worker_process()
        with owner.batch(on_finish=RaceMember) as batch:
            RaceMember.perform_later(owner, "holder")
        holder = owner.drain_batch(1)
        assert len(holder) == 1
        adding = adder.find_batch(batch.id)
        assert adding is not None
        written_inside_transaction = threading.Event()
        allow_commit = threading.Event()

        def append():
            with adding.enqueue():
                adder.perform_all_later([RaceMember.build("new")])
                written_inside_transaction.set()
                assert allow_commit.wait(timeout=10)

        with ThreadPoolExecutor(max_workers=2) as pool:
            append_future = pool.submit(append)
            assert written_inside_transaction.wait(timeout=10)
            finish_future = pool.submit(holder[0].perform)
            time.sleep(0.05)
            allow_commit.set()
            append_future.result(timeout=15)
            finish_future.result(timeout=15)

        assert batch.reload().total_jobs == 2
        assert not batch.finished
        assert count("jobs") == 2
        assert count("jobs", "WHERE finished_at IS NOT NULL") == 1
        assert count("ready_executions") == 1
        assert count("batch_executions") == 1

        new_job = owner.drain_batch(1)
        assert len(new_job) == 1
        new_job[0].perform()
        assert batch.reload().finished
        assert count("ready_executions") == 1
        with pytest.raises(quebec.BatchAlreadyFinished):
            with adding.enqueue():
                adder.perform_all_later([RaceMember.build("too late")])
        callback = owner.drain_batch(1)
        assert len(callback) == 1
        callback[0].perform()
        assert count("jobs") == 3
        assert count("jobs", "WHERE finished_at IS NOT NULL") == 3
        for table in (
            "ready_executions",
            "batch_executions",
            "claimed_executions",
            "failed_executions",
        ):
            assert count(table) == 0
    finally:
        observer.close()
        adder.close()
        owner.close()

"""Live MySQL concurrent appends to one running batch."""

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote, urlsplit

import pytest
import quebec


class BatchMember(quebec.BaseClass):
    queue_as = "perf"

    def perform(self, value=None):
        return None


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_four_producers_append_to_one_running_batch_without_losing_members():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    prefix = f"mbaddtest_{uuid.uuid4().hex[:10]}"
    instances = [
        quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
        for _ in range(4)
    ]
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
        instances[0].create_tables()
        for instance in instances:
            instance.register_job(BatchMember)
        instances[0].register_worker_process()
        with instances[0].batch(on_finish=BatchMember) as batch:
            BatchMember.perform_later(instances[0], "holder")

        handles = [instance.find_batch(batch.id) for instance in instances]
        assert all(handle is not None for handle in handles)
        gate = threading.Barrier(4)

        def append(index):
            jobs = [
                BatchMember.build(value) for value in range(index * 8, (index + 1) * 8)
            ]
            gate.wait(timeout=10)
            with handles[index].enqueue():
                instances[index].perform_all_later(jobs)

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(append, range(4)))
        assert batch.reload().total_jobs == 33
        assert count("jobs") == 33
        assert count("ready_executions") == 33
        assert count("batch_executions") == 33

        executions = instances[0].drain_batch(33)
        assert len(executions) == 33
        for execution in executions:
            execution.perform()
        assert batch.reload().finished
        assert count("ready_executions") == 1
        callback = instances[0].drain_batch(1)
        assert len(callback) == 1
        callback[0].perform()
        assert count("jobs", "WHERE finished_at IS NOT NULL") == 34
        for table in ("batch_executions", "claimed_executions", "failed_executions"):
            assert count(table) == 0
    finally:
        observer.close()
        for instance in instances:
            instance.close()

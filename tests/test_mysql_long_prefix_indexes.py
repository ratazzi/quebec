"""Live MySQL long-prefix indexes protect concurrent blocked maintenance."""

import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote, urlsplit

import pytest

import quebec


class LongPrefixSweep(quebec.BaseClass):
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(value):
        return f"resource-{value}"

    def perform(self, value):
        return None


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_long_prefix_indexes_and_two_dispatchers():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    if parsed.scheme != "mysql":
        raise ValueError("TEST_MYSQL_URL must use mysql://")
    prefix = f"mbu_expired_16_2_1_{uuid.uuid4().hex[:6]}"
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )

    def make_qc():
        qc = quebec.Quebec(
            dsn,
            table_name_prefix=prefix,
            use_listen_notify=False,
            dispatcher_polling_interval=10,
            dispatcher_concurrency_maintenance=True,
            dispatcher_concurrency_maintenance_interval=600,
            dispatcher_batch_maintenance=False,
        )
        qc.register_job(LongPrefixSweep)
        return qc

    first = make_qc()
    second = make_qc()

    def count(table):
        with observer.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {prefix}_{table}")
            return cursor.fetchone()[0]

    try:
        first.create_tables()
        first.create_tables()  # Generated names must be stable for idempotent setup.
        expected = {
            "blocked_executions": {("concurrency_key", "priority", "job_id")},
            "ready_executions": {
                ("queue_name", "priority", "job_id"),
                ("priority", "job_id"),
            },
            "recurring_executions": {("task_key", "run_at")},
        }
        with observer.cursor() as cursor:
            for table, required in expected.items():
                cursor.execute(f"SHOW INDEX FROM {prefix}_{table}")
                indexes = {}
                for row in cursor.fetchall():
                    indexes.setdefault(row[2], []).append((row[3], row[4]))
                for name in indexes:
                    assert len(name) <= 64
                actual = {
                    tuple(column for _position, column in sorted(columns))
                    for columns in indexes.values()
                }
                assert required <= actual
        first.perform_all_later(
            [LongPrefixSweep.build(value // 2) for value in range(32)]
        )
        assert count("ready_executions") == 16
        assert count("blocked_executions") == 16
        past = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=1)
        with observer.cursor() as cursor:
            cursor.execute(
                f"UPDATE {prefix}_blocked_executions SET expires_at=%s", (past,)
            )
            cursor.execute(f"DELETE FROM {prefix}_ready_executions")
            cursor.execute(f"UPDATE {prefix}_semaphores SET expires_at=%s", (past,))
        first.spawn_dispatcher()
        second.spawn_dispatcher()
        deadline = time.perf_counter() + 5
        while count("ready_executions") != 16 or count("blocked_executions") != 0:
            if time.perf_counter() >= deadline:
                raise AssertionError(
                    "two dispatchers did not promote each expired key exactly once"
                )
            time.sleep(0.02)
        assert count("semaphores") == 16
        assert count("failed_executions") == 0
        first.register_worker_process()
        claimed = first.drain_batch(4)
        assert len(claimed) == 4
        with observer.cursor() as cursor:
            cursor.execute(f"SHOW INDEX FROM {prefix}_ready_executions")
            priority_index = next(
                row[2]
                for row in cursor.fetchall()
                if row[4] == "priority" and row[3] == 1 and row[2].startswith("idx_")
            )
            cursor.execute(
                "SELECT SQL_TEXT FROM performance_schema.prepared_statements_instances "
                "WHERE SQL_TEXT LIKE %s",
                (f"%{prefix}_ready_executions%",),
            )
            assert any(
                sql and f"INDEX(r `{priority_index}`)" in sql
                for (sql,) in cursor.fetchall()
            )
        for execution in claimed:
            execution.perform()
    finally:
        second.close()
        first.close()
        observer.close()

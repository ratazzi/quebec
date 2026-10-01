"""Live MySQL claim index hint and legacy missing-index fallback."""

import os
import uuid
from urllib.parse import unquote, urlsplit

import pytest

import quebec


class MySqlClaimWork(quebec.BaseClass):
    queue_as = "original"

    def perform(self, value):
        return None


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_exact_queue_claim_survives_missing_optional_index():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    if parsed.scheme != "mysql":
        raise ValueError("TEST_MYSQL_URL must use mysql://")
    prefix = f"mi_{uuid.uuid4().hex[:10]}"
    qc = quebec.Quebec(
        dsn,
        table_name_prefix=prefix,
        use_listen_notify=False,
        force_override_queue="fast",
    )
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
        qc.register_job(MySqlClaimWork)
        qc.register_worker_process()
        qc.perform_all_later([MySqlClaimWork.build(value) for value in range(16)])
        first = qc.drain_batch(8)
        assert len(first) == 8
        assert all(execution.queue == "fast" for execution in first)
        with observer.cursor() as cursor:
            cursor.execute(
                "SELECT SQL_TEXT FROM performance_schema.prepared_statements_instances "
                "WHERE SQL_TEXT LIKE %s",
                (f"%{prefix}_ready_executions%",),
            )
            texts = [sql for (sql,) in cursor.fetchall() if sql]
            assert any(
                f"INDEX(r `idx_{prefix}_ready_executions_queue_priority_job`)" in sql
                and "WHERE `queue_name` = ?" in sql
                for sql in texts
            )
            cursor.execute(
                f"DROP INDEX idx_{prefix}_ready_executions_queue_priority_job "
                f"ON {prefix}_ready_executions"
            )
        second = qc.drain_batch(8)
        assert len(second) == 8
        assert {execution.id for execution in first}.isdisjoint(
            execution.id for execution in second
        )
        for execution in first + second:
            execution.perform()
        with observer.cursor() as cursor:
            cursor.execute(
                f"SELECT count(*) FROM {prefix}_jobs WHERE finished_at IS NOT NULL"
            )
            assert cursor.fetchone()[0] == 16
            for table in (
                "ready_executions",
                "claimed_executions",
                "failed_executions",
            ):
                cursor.execute(f"SELECT count(*) FROM {prefix}_{table}")
                assert cursor.fetchone()[0] == 0
    finally:
        observer.close()
        qc.close()

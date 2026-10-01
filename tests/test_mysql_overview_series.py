"""Live MySQL overview chart counts and grouped-query regression."""

import html
import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote, urlsplit

import pytest
import quebec


class MySqlOverviewWork(quebec.BaseClass):
    def perform(self, value):
        return None


class MySqlPerfQueueWork(quebec.BaseClass):
    queue_as = "perf"

    def perform(self, value):
        return None


class MySqlOtherQueueWork(quebec.BaseClass):
    queue_as = "other"

    def perform(self, value):
        return None


def _prepared_statement_counts(observer, prefix):
    # Statement IDs are connection-local. Include the owning thread and match
    # the literal unique table prefix so other clients cannot affect the budget.
    with observer.cursor() as cursor:
        cursor.execute(
            "SELECT OWNER_THREAD_ID, STATEMENT_ID, SQL_TEXT, COUNT_EXECUTE "
            "FROM performance_schema.prepared_statements_instances "
            "WHERE LOCATE(%s, SQL_TEXT) > 0",
            (f"{prefix}_",),
        )
        return {
            (thread_id, statement_id): (sql, count)
            for thread_id, statement_id, sql, count in cursor.fetchall()
        }


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_overview_chart_uses_one_grouped_read():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    if parsed.scheme != "mysql":
        raise ValueError("TEST_MYSQL_URL must use mysql://")
    prefix = f"mo_{uuid.uuid4().hex[:10]}"
    qc = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
    unrelated_qc = quebec.Quebec(
        dsn,
        table_name_prefix=f"noise_{uuid.uuid4().hex[:10]}",
        use_listen_notify=False,
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
        qc.register_job(MySqlOverviewWork)
        unrelated_qc.create_tables()
        unrelated_qc.register_job(MySqlOverviewWork)
        qc.perform_all_later([MySqlOverviewWork.build(value) for value in range(3)])
        with observer.cursor() as cursor:
            cursor.execute(f"SELECT id FROM {prefix}_jobs ORDER BY id")
            ids = [row[0] for row in cursor.fetchall()]
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            for job_id, hours_ago in zip(ids, (3, 10, 200), strict=True):
                finished = now - timedelta(hours=hours_ago)
                cursor.execute(
                    f"UPDATE {prefix}_jobs SET created_at=%s, finished_at=%s WHERE id=%s",
                    (finished - timedelta(seconds=1), finished, job_id),
                )
            cursor.execute(f"DELETE FROM {prefix}_ready_executions")
        qc.handle_control_plane_request(
            quebec.AsgiRequest("GET", "/health", "", [], b"", "/quebec")
        )
        for hours, expected_sum, expected_length in (
            (24, 2, 24),
            (168, 2, 28),
            (720, 3, 30),
        ):
            before = _prepared_statement_counts(observer, prefix)
            # These binary-protocol executions used to pollute the global
            # counter, even though they touch none of this request's tables.
            unrelated_qc.perform_all_later(
                [MySqlOverviewWork.build(value) for value in range(40)]
            )
            request = quebec.AsgiRequest(
                "GET", "/", f"hours={hours}", [], b"", "/quebec"
            )
            status, _headers, body = qc.handle_control_plane_request(request)
            assert status == 200
            match = re.search(
                r"data-jobs-processed-data='([^']*)'", bytes(body).decode()
            )
            assert match is not None
            counts = json.loads(html.unescape(match.group(1)))
            assert len(counts) == expected_length
            assert sum(counts) == expected_sum
            assert sorted(value for value in counts if value) == (
                [1, 1] if hours in (24, 168) else [1, 2]
            )
            after = _prepared_statement_counts(observer, prefix)
            executions = [
                (sql, count - before.get(key, (None, 0))[1])
                for key, (sql, count) in after.items()
            ]
            assert 0 < sum(count for _sql, count in executions) < 30
            assert (
                sum(
                    count
                    for sql, count in executions
                    if "TIMESTAMPDIFF(MICROSECOND" in sql.upper()
                )
                == 1
            )
    finally:
        observer.close()
        qc.close()
        unrelated_qc.close()


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_overview_queue_failure_rates_keep_queue_and_time_boundaries():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    prefix = f"mo_{uuid.uuid4().hex[:10]}"
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
        qc.register_job(MySqlPerfQueueWork)
        qc.register_job(MySqlOtherQueueWork)
        qc.perform_all_later(
            [MySqlPerfQueueWork.build(i) for i in range(4)]
            + [MySqlOtherQueueWork.build(i) for i in range(2)]
        )
        with observer.cursor() as cursor:
            cursor.execute(f"SELECT id, queue_name FROM {prefix}_jobs ORDER BY id")
            ids = {"perf": [], "other": []}
            for job_id, queue_name in cursor.fetchall():
                ids[queue_name].append(job_id)
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            finished = ids["perf"][:2] + ids["other"][:1]
            for job_id in finished:
                cursor.execute(
                    f"UPDATE {prefix}_jobs SET finished_at=%s WHERE id=%s",
                    (now, job_id),
                )
                cursor.execute(
                    f"DELETE FROM {prefix}_ready_executions WHERE job_id=%s", (job_id,)
                )
            old_id = ids["perf"][3]
            cursor.execute(
                f"UPDATE {prefix}_jobs SET created_at=%s WHERE id=%s",
                (now - timedelta(days=31), old_id),
            )
            for job_id in (ids["perf"][0], old_id, ids["other"][1]):
                cursor.execute(
                    f"INSERT INTO {prefix}_failed_executions (job_id, error, created_at) "
                    "VALUES (%s, %s, %s)",
                    (job_id, "fixture failure", now),
                )

        request = quebec.AsgiRequest("GET", "/", "hours=24", [], b"", "/quebec")
        status, _headers, body = qc.handle_control_plane_request(request)
        assert status == 200
        rendered = bytes(body).decode()
        for name, processed, failed_rate in (("perf", 2, 33), ("other", 1, 50)):
            match = re.search(
                rf">{name}</a>\s*</td>\s*<td[^>]*>(\d+)</td>\s*"
                rf"<td[^>]*>[^<]*</td>\s*<td[^>]*>(\d+)%</td>",
                rendered,
            )
            assert match is not None, f"missing queue performance row for {name}"
            assert (int(match[1]), int(match[2])) == (processed, failed_rate)
    finally:
        observer.close()
        qc.close()


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_overview_class_ties_use_stable_labels():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    prefix = f"mo_{uuid.uuid4().hex[:10]}"
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
        qc.register_job(MySqlOverviewWork)
        qc.perform_all_later([MySqlOverviewWork.build(value) for value in range(8)])
        with observer.cursor() as cursor:
            cursor.execute(
                f"UPDATE {prefix}_jobs SET class_name = "
                "CONCAT('Class', LPAD(MOD(id, 8), 2, '0'))"
            )
        request = quebec.AsgiRequest("GET", "/", "hours=24", [], b"", "/quebec")
        status, _headers, body = qc.handle_control_plane_request(request)
        assert status == 200
        match = re.search(r"data-job-types-labels='([^']*)'", bytes(body).decode())
        assert match is not None
        labels = json.loads(html.unescape(match[1]))
        assert labels == [f"Class{number:02d}" for number in range(7)]
        with observer.cursor() as cursor:
            cursor.execute(
                "SELECT SQL_TEXT FROM performance_schema.prepared_statements_instances "
                "WHERE SQL_TEXT LIKE %s",
                (f"%{prefix}_jobs%",),
            )
            assert any(
                sql and "FORCE INDEX(PRIMARY)" in sql.upper()
                for (sql,) in cursor.fetchall()
            )
    finally:
        observer.close()
        qc.close()

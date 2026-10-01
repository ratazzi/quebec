"""Live MySQL queue-slot plus rate-token concurrency invariant."""

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from urllib.parse import unquote, urlsplit

import pytest

import quebec


class MySqlRateJob(quebec.BaseClass):
    queue_as = "default"
    rate_limit_max = 1
    rate_limit_duration = timedelta(seconds=60)

    def perform(self, value):
        return None


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_rate_throttle_returns_queue_slot_under_four_claimers():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    if parsed.scheme != "mysql":
        raise ValueError("TEST_MYSQL_URL must use mysql://")
    prefix = f"mg_{uuid.uuid4().hex[:10]}"
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
            use_skip_locked=True,
            experimental_queue_concurrency={"default": 1},
        )
        qc.register_job(MySqlRateJob)
        return qc

    producer = make_qc()
    claimers = []
    try:
        producer.create_tables()
        producer.register_worker_process()
        producer.perform_all_later([MySqlRateJob.build(value) for value in range(8)])
        prime = producer.drain_batch(1)
        assert len(prime) == 1
        prime[0].perform()
        for _ in range(4):
            qc = make_qc()
            qc.register_worker_process()
            claimers.append(qc)
        barrier = threading.Barrier(4)

        def drain(qc):
            barrier.wait(timeout=10)
            return qc.drain_batch(1)

        with ThreadPoolExecutor(max_workers=4) as pool:
            batches = list(pool.map(drain, claimers))
        assert not any(batches)
        with observer.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {prefix}_scheduled_executions")
            assert cursor.fetchone()[0] == 7
            for table in (
                "ready_executions",
                "claimed_executions",
                "failed_executions",
            ):
                cursor.execute(f"SELECT count(*) FROM {prefix}_{table}")
                assert cursor.fetchone()[0] == 0
            cursor.execute(
                f"SELECT value FROM {prefix}_semaphores WHERE `key`=%s",
                ("queue:default",),
            )
            assert cursor.fetchone() == (1,)
    finally:
        for qc in claimers:
            qc.close()
        producer.close()
        observer.close()


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_concurrent_orphan_recovery_releases_queue_slot_once():
    """A peer supervisor must not free the slot held by a still-live claim."""
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    if parsed.scheme != "mysql":
        raise ValueError("TEST_MYSQL_URL must use mysql://")
    prefix = f"mor_{uuid.uuid4().hex[:10]}"
    orphan_count = 128
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )
    supervisors = [
        quebec.Quebec(
            dsn,
            table_name_prefix=prefix,
            use_listen_notify=False,
            experimental_queue_concurrency={"default": orphan_count + 1},
        )
        for _ in range(2)
    ]
    try:
        supervisors[0].create_tables()
        with observer.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {prefix}_processes "
                "(kind, last_heartbeat_at, pid, hostname, created_at, name) "
                "VALUES ('Worker', NOW(6), 12345, 'live', NOW(6), 'live')"
            )
            live_process_id = cursor.lastrowid
            for index in range(orphan_count + 1):
                cursor.execute(
                    f"INSERT INTO {prefix}_jobs "
                    "(queue_name, class_name, arguments, priority, active_job_id, created_at, updated_at) "
                    "VALUES ('default', 'CrashJob', '[]', 0, %s, NOW(6), NOW(6))",
                    (f"crash-{index}",),
                )
                job_id = cursor.lastrowid
                cursor.execute(
                    f"INSERT INTO {prefix}_claimed_executions "
                    "(job_id, process_id, created_at) VALUES (%s, %s, NOW(6))",
                    (job_id, live_process_id if index == orphan_count else None),
                )
            cursor.execute(
                f"INSERT INTO {prefix}_semaphores "
                "(`key`, value, expires_at, created_at, updated_at) "
                "VALUES ('queue:default', 0, DATE_ADD(NOW(6), INTERVAL 1 HOUR), NOW(6), NOW(6))"
            )

        barrier = threading.Barrier(3)

        def maintain(qc):
            barrier.wait(timeout=10)
            return qc.supervisor_run_maintenance(None)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(maintain, qc) for qc in supervisors]
            barrier.wait(timeout=10)
            results = [future.result(timeout=30) for future in futures]

        assert sum(orphaned for _, orphaned in results) == orphan_count
        with observer.cursor() as cursor:
            cursor.execute(
                f"SELECT value FROM {prefix}_semaphores WHERE `key`=%s",
                ("queue:default",),
            )
            assert cursor.fetchone() == (orphan_count,)
            cursor.execute(f"SELECT count(*) FROM {prefix}_claimed_executions")
            assert cursor.fetchone()[0] == 1
            cursor.execute(f"SELECT count(*) FROM {prefix}_failed_executions")
            assert cursor.fetchone()[0] == orphan_count
    finally:
        for qc in supervisors:
            qc.close()
        observer.close()


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_orphan_recovery_retries_one_failed_row():
    """A failed row stays claimed while a sibling succeeds, then retries later."""
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    if parsed.scheme != "mysql":
        raise ValueError("TEST_MYSQL_URL must use mysql://")
    prefix = f"mretry_{uuid.uuid4().hex[:10]}"
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )
    qc = quebec.Quebec(
        dsn,
        table_name_prefix=prefix,
        use_listen_notify=False,
        experimental_queue_concurrency={"locked": 1},
    )
    try:
        qc.create_tables()
        with observer.cursor() as cursor:
            for queue_name in ("locked", "free"):
                cursor.execute(
                    f"INSERT INTO {prefix}_jobs "
                    "(queue_name, class_name, arguments, priority, active_job_id, created_at, updated_at) "
                    "VALUES (%s, 'CrashJob', '[]', 0, %s, NOW(6), NOW(6))",
                    (queue_name, queue_name),
                )
                cursor.execute(
                    f"INSERT INTO {prefix}_claimed_executions "
                    "(job_id, process_id, created_at) VALUES (%s, NULL, NOW(6))",
                    (cursor.lastrowid,),
                )
            cursor.execute(f"DROP TABLE {prefix}_semaphores")

        assert qc.supervisor_run_maintenance(None) == (0, 1)
        with observer.cursor() as cursor:
            cursor.execute(
                f"SELECT j.queue_name FROM {prefix}_claimed_executions c "
                f"JOIN {prefix}_jobs j ON j.id = c.job_id"
            )
            assert cursor.fetchall() == (("locked",),)

        qc.create_tables()
        with observer.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {prefix}_semaphores "
                "(`key`, value, expires_at, created_at, updated_at) "
                "VALUES ('queue:locked', 0, DATE_ADD(NOW(6), INTERVAL 1 HOUR), "
                "NOW(6), NOW(6))"
            )
        assert qc.supervisor_run_maintenance(None) == (0, 1)
        with observer.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {prefix}_claimed_executions")
            assert cursor.fetchone()[0] == 0
            cursor.execute(f"SELECT count(*) FROM {prefix}_failed_executions")
            assert cursor.fetchone()[0] == 2
            cursor.execute(
                f"SELECT value FROM {prefix}_semaphores WHERE `key`=%s",
                ("queue:locked",),
            )
            assert cursor.fetchone() == (1,)
    finally:
        qc.close()
        observer.close()


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_concurrent_stale_prune_reports_distinct_deletes():
    """Maintenance counts process rows actually deleted under eight-way races."""
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    if parsed.scheme != "mysql":
        raise ValueError("TEST_MYSQL_URL must use mysql://")
    prefix = f"mprune_{uuid.uuid4().hex[:10]}"
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )
    supervisors = [
        quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
        for _ in range(8)
    ]
    try:
        supervisors[0].create_tables()
        with observer.cursor() as cursor:
            for process_number in range(16):
                cursor.execute(
                    f"INSERT INTO {prefix}_processes "
                    "(kind, last_heartbeat_at, pid, hostname, created_at, name) "
                    "VALUES ('Worker', '2000-01-01', %s, 'stale', '2000-01-01', %s)",
                    (20_000 + process_number, f"Worker-{process_number}"),
                )
                process_id = cursor.lastrowid
                for job_number in range(8):
                    cursor.execute(
                        f"INSERT INTO {prefix}_jobs "
                        "(queue_name, class_name, arguments, priority, active_job_id, created_at, updated_at) "
                        "VALUES ('default', 'CrashJob', '[]', 0, %s, NOW(6), NOW(6))",
                        (f"crash-{process_number}-{job_number}",),
                    )
                    cursor.execute(
                        f"INSERT INTO {prefix}_claimed_executions "
                        "(job_id, process_id, created_at) VALUES (%s, %s, NOW(6))",
                        (cursor.lastrowid, process_id),
                    )
        barrier = threading.Barrier(len(supervisors) + 1)

        def maintain(qc):
            barrier.wait(timeout=10)
            return qc.supervisor_run_maintenance(None)

        with ThreadPoolExecutor(max_workers=len(supervisors)) as pool:
            futures = [pool.submit(maintain, qc) for qc in supervisors]
            barrier.wait(timeout=10)
            results = [future.result(timeout=30) for future in futures]
        assert sum(pruned for pruned, _ in results) == 16
        with observer.cursor() as cursor:
            for table, expected in (
                ("processes", 0),
                ("claimed_executions", 0),
                ("failed_executions", 128),
            ):
                cursor.execute(f"SELECT count(*) FROM {prefix}_{table}")
                assert cursor.fetchone()[0] == expected
    finally:
        for qc in supervisors:
            qc.close()
        observer.close()

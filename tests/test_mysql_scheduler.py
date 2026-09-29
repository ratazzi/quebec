"""Live MySQL recurring sync and duplicate-scheduler tick regression."""

import os
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest

import quebec


class MySqlScheduledWork(quebec.BaseClass):
    queue_as = "perf"

    def perform(self, value=None):
        return None


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_long_prefix_two_schedulers_enqueue_each_tick_once():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    if parsed.scheme != "mysql":
        raise ValueError("TEST_MYSQL_URL must use mysql://")
    prefix = f"mrecurring_long_2_1_{uuid.uuid4().hex[:6]}"
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )
    first = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
    second = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
    old_schedule = os.environ.get("QUEBEC_RECURRING_SCHEDULE")
    old_env = os.environ.get("QUEBEC_ENV")
    schedule_path = None

    def count(table):
        with observer.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {prefix}_{table}")
            return cursor.fetchone()[0]

    try:
        first.create_tables()
        first.register_job(MySqlScheduledWork)
        second.register_job(MySqlScheduledWork)
        with observer.cursor() as cursor:
            cursor.execute(f"SHOW INDEX FROM {prefix}_recurring_executions")
            indexes = {}
            for row in cursor.fetchall():
                indexes.setdefault((row[2], row[1]), []).append((row[3], row[4]))
            assert any(
                non_unique == 0
                and [column for _position, column in sorted(columns)]
                == ["task_key", "run_at"]
                and len(name) <= 64
                for (name, non_unique), columns in indexes.items()
            )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as file:
            file.write(
                "test:\n  tick:\n    class: MySqlScheduledWork\n"
                "    schedule: every second\n    queue: perf\n"
            )
            schedule_path = file.name
        os.environ["QUEBEC_RECURRING_SCHEDULE"] = schedule_path
        os.environ["QUEBEC_ENV"] = "test"
        first.spawn_scheduler()
        second.spawn_scheduler()
        deadline = time.perf_counter() + 8
        while count("recurring_tasks") != 1 or count("recurring_executions") < 3:
            if time.perf_counter() >= deadline:
                raise AssertionError(
                    "MySQL schedulers did not sync and enqueue three ticks"
                )
            time.sleep(0.02)
        time.sleep(0.1)
        recurring = count("recurring_executions")
        assert recurring == count("jobs") == count("ready_executions")
        with observer.cursor() as cursor:
            cursor.execute(
                f"SELECT count(*), count(DISTINCT task_key, run_at) "
                f"FROM {prefix}_recurring_executions"
            )
            assert cursor.fetchone() == (recurring, recurring)
    finally:
        second.close()
        first.close()
        observer.close()
        if schedule_path is not None:
            Path(schedule_path).unlink()
        if old_schedule is None:
            os.environ.pop("QUEBEC_RECURRING_SCHEDULE", None)
        else:
            os.environ["QUEBEC_RECURRING_SCHEDULE"] = old_schedule
        if old_env is None:
            os.environ.pop("QUEBEC_ENV", None)
        else:
            os.environ["QUEBEC_ENV"] = old_env


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_surviving_scheduler_continues_after_peer_closes():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    prefix = f"mrecfailtest_{uuid.uuid4().hex[:10]}"
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )
    first = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
    second = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
    old_schedule = os.environ.get("QUEBEC_RECURRING_SCHEDULE")
    old_env = os.environ.get("QUEBEC_ENV")
    schedule_path = None
    first_closed = False

    def count(table):
        with observer.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {prefix}_{table}")
            return cursor.fetchone()[0]

    def wait_for_ticks(target):
        deadline = time.perf_counter() + 8
        while count("recurring_executions") < target:
            if time.perf_counter() >= deadline:
                raise AssertionError(f"MySQL scheduler did not reach tick {target}")
            time.sleep(0.02)

    try:
        first.create_tables()
        first.register_job(MySqlScheduledWork)
        second.register_job(MySqlScheduledWork)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as file:
            file.write(
                "test:\n  tick:\n    class: MySqlScheduledWork\n"
                "    schedule: every second\n    queue: perf\n"
            )
            schedule_path = file.name
        os.environ["QUEBEC_RECURRING_SCHEDULE"] = schedule_path
        os.environ["QUEBEC_ENV"] = "test"
        first.spawn_scheduler()
        second.spawn_scheduler()
        wait_for_ticks(2)
        first.close()
        first_closed = True
        at_close = count("recurring_executions")
        wait_for_ticks(at_close + 3)
        with observer.cursor() as cursor:
            cursor.execute(
                f"SELECT run_at FROM {prefix}_recurring_executions ORDER BY run_at"
            )
            run_ats = [run_at for (run_at,) in cursor.fetchall()]
        assert all(
            (right - left).total_seconds() == 1
            for left, right in zip(run_ats, run_ats[1:])
        )
        assert len(run_ats) == count("jobs") == count("ready_executions")
    finally:
        second.close()
        if not first_closed:
            first.close()
        observer.close()
        if schedule_path is not None:
            Path(schedule_path).unlink()
        if old_schedule is None:
            os.environ.pop("QUEBEC_RECURRING_SCHEDULE", None)
        else:
            os.environ["QUEBEC_RECURRING_SCHEDULE"] = old_schedule
        if old_env is None:
            os.environ.pop("QUEBEC_ENV", None)
        else:
            os.environ["QUEBEC_ENV"] = old_env

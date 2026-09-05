"""A transient heartbeat write failure must not stop scheduled-job dispatch."""

import sqlite3
import time

import quebec

from .helpers import wait_until


def test_dispatcher_recovers_after_heartbeat_error(temp_db_path, test_prefix):
    qc = quebec.Quebec(
        f"sqlite:///{temp_db_path}?mode=rwc",
        table_name_prefix=test_prefix,
        process_heartbeat_interval=0.05,
        dispatcher_polling_interval=0.02,
    )
    qc.create_tables()

    class ScheduledJob(quebec.BaseClass):
        def perform(self):
            pass

    qc.register_job(ScheduledJob)
    ScheduledJob.set(wait=3600).perform_later(qc)
    sql = sqlite3.connect(temp_db_path)
    try:
        sql.execute(f"""
            CREATE TRIGGER fail_heartbeat BEFORE UPDATE ON {test_prefix}_processes
            BEGIN SELECT RAISE(FAIL, 'transient heartbeat failure'); END
        """)
        sql.commit()
        qc.spawn_dispatcher()
        wait_until(
            lambda: sql.execute(f"SELECT COUNT(*) FROM {test_prefix}_processes").fetchone()[0] == 1,
            timeout=2,
        )
        # Leave the fault active for multiple heartbeat ticks, including the
        # immediate first tick, then restore writes without restarting anything.
        time.sleep(0.2)
        sql.execute("DROP TRIGGER fail_heartbeat")
        sql.execute(f"UPDATE {test_prefix}_scheduled_executions SET scheduled_at = '2020-01-01 00:00:00'")
        sql.commit()
        wait_until(
            lambda: sql.execute(f"SELECT COUNT(*) FROM {test_prefix}_ready_executions").fetchone()[0] == 1,
            timeout=2,
            message="dispatcher stopped after heartbeat failure",
        )
        assert sql.execute(f"SELECT COUNT(*) FROM {test_prefix}_scheduled_executions").fetchone()[0] == 0
    finally:
        sql.close()
        qc.close()

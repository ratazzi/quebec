"""A transient heartbeat write failure must not stop scheduled-job dispatch."""

from datetime import datetime, timedelta, timezone

import quebec
from sqlalchemy import text

from .helpers import create_database_engine, database_engine, observe_sqlite, wait_until


def test_dispatcher_recovers_after_heartbeat_error(db_url, test_prefix):
    qc = quebec.Quebec(
        db_url,
        table_name_prefix=test_prefix,
        process_heartbeat_interval=0.05,
        dispatcher_polling_interval=0.02,
    )
    qc.create_tables()

    class ScheduledJob(quebec.BaseClass):
        def perform(self):
            pass

    qc.register_job(ScheduledJob)
    # Falls due only after the injected fault has healed, so the dispatcher has
    # to survive the failing heartbeats to ever promote it.
    ScheduledJob.set(wait=2).perform_later(qc)

    # Fail every heartbeat for the first second, then heal on the database
    # clock. The fault has to expire on its own: dropping the trigger from here
    # while the dispatcher runs would race Quebec's own writes, because the two
    # sqlite libraries in this process cannot see each other's file locks.
    fault_until = datetime.now(timezone.utc) + timedelta(seconds=1)
    with database_engine(db_url) as engine, engine.begin() as setup:
        if setup.dialect.name == "postgresql":
            setup.execute(text(f"""
                CREATE FUNCTION fail_heartbeat() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF clock_timestamp() < TIMESTAMPTZ '{fault_until.isoformat()}' THEN
                        RAISE EXCEPTION 'transient heartbeat failure';
                    END IF;
                    RETURN NEW;
                END $$
            """))
            setup.execute(text(f"""
                CREATE TRIGGER fail_heartbeat BEFORE UPDATE ON {test_prefix}_processes
                FOR EACH ROW EXECUTE FUNCTION fail_heartbeat()
            """))
        else:
            setup.execute(text(f"""
                CREATE TRIGGER fail_heartbeat BEFORE UPDATE ON {test_prefix}_processes
                WHEN julianday('now') < julianday('{fault_until:%Y-%m-%d %H:%M:%S.%f}')
                BEGIN SELECT RAISE(FAIL, 'transient heartbeat failure'); END
            """))

    engine = create_database_engine(db_url, readonly=True)
    sql = engine.connect()

    def count(table):
        return observe_sqlite(
            lambda: sql.execute(
                text(f"SELECT COUNT(*) FROM {test_prefix}_{table}")
            ).fetchone()[0]
        )

    try:
        qc.spawn_dispatcher()
        wait_until(lambda: count("processes") == 1, timeout=2)
        wait_until(
            lambda: count("ready_executions") == 1,
            timeout=5,
            message="dispatcher stopped after heartbeat failure",
        )
        assert count("scheduled_executions") == 0
    finally:
        sql.close()
        engine.dispose()
        qc.close()

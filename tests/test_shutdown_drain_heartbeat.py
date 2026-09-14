"""The heartbeat has to keep beating while a worker drains.

Quebec's heartbeat lives on the main loop's `select!`, but the drain runs after
that loop exits — so a draining worker used to go silent. Solid Queue keeps
beating for the whole drain (`after_shutdown :stop_heartbeat`), and it has to:
once a drain outlasts `process_alive_threshold` the row is pruned and the jobs
still inside perform() are handed to another worker, which is precisely the
double execution the drain exists to prevent.
"""

from __future__ import annotations

import os
import signal
import time

import pytest
import quebec

from .helpers import observe_sqlite, readonly_connect, wait_until

SHUTDOWN_GRACE = 30.0
JOB_SECONDS = 4.0

pytestmark = [
    pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork()"),
    pytest.mark.filterwarnings("ignore:.*multi-threaded.*fork:DeprecationWarning"),
]


class SlowJob(quebec.BaseClass):
    def perform(self, marker: str, seconds: float) -> None:
        with open(marker, "w") as fh:
            fh.write("running")
        time.sleep(seconds)


def _heartbeat_of(conn, prefix: str):
    return observe_sqlite(
        lambda: conn.execute(
            f"SELECT last_heartbeat_at FROM {prefix}_processes WHERE kind = 'Worker'"
        ).fetchone()
    )


def test_the_heartbeat_keeps_beating_while_draining(
    temp_db_path, test_prefix, tmp_path
) -> None:
    marker = tmp_path / "job-started"
    qc = quebec.Quebec(
        f"sqlite:///{temp_db_path}?mode=rwc",
        table_name_prefix=test_prefix,
        process_heartbeat_interval=0.1,
        # Long enough that the drain is still running well after SIGTERM, so
        # the beats under test actually have to happen during it.
        shutdown_timeout=JOB_SECONDS + 4,
    )
    assert qc.create_tables()
    qc.register_job(SlowJob)
    SlowJob.perform_later(qc, str(marker), JOB_SECONDS)

    child = os.fork()
    if child == 0:
        try:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            qc.reset_after_fork()
            qc.run(spawn=["worker"], create_tables=False)
            os._exit(0)
        except BaseException:  # noqa: BLE001 - must not unwind into pytest
            os._exit(1)

    sql = readonly_connect(temp_db_path)
    try:
        # Wait until the job is actually inside perform(), so SIGTERM lands
        # while there is something to drain.
        wait_until(
            marker.exists,
            timeout=20,
            interval=0.05,
            message="the job never started",
        )
        before = _heartbeat_of(sql, test_prefix)
        assert before is not None, "worker never registered"

        os.kill(child, signal.SIGTERM)

        # The job still has seconds to go; the drain is now what keeps the
        # process alive, and it must keep the row fresh while it waits.
        wait_until(
            lambda: _heartbeat_of(sql, test_prefix) != before,
            timeout=JOB_SECONDS,
            interval=0.05,
            message="the heartbeat stopped as soon as the drain began",
        )
    finally:
        sql.close()
        deadline = time.monotonic() + SHUTDOWN_GRACE
        while time.monotonic() < deadline:
            waited, _status = os.waitpid(child, os.WNOHANG)
            if waited == child:
                break
            time.sleep(0.05)
        else:
            os.kill(child, signal.SIGKILL)
            os.waitpid(child, 0)
        qc.close()

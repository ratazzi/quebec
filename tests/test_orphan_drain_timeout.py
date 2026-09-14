"""An orphaned worker drains on its own budget.

`shutdown_timeout` is short because a live supervisor SIGKILLs whatever is left
when it expires. Nobody is holding that deadline over an orphan, so cutting its
jobs off after the same few seconds only leaves them claimed until the process
row expires. `orphan_drain_timeout` covers that path instead.

Both tests give the job more time than `shutdown_timeout` allows, so whether it
reaches the end is a direct read on which budget was used.
"""

from __future__ import annotations

import os
import signal
import time

import pytest
import quebec

from .helpers import wait_until

JOB_SECONDS = 3.0
SHORT_SHUTDOWN = 1.0
REAP_GRACE = 30.0

pytestmark = [
    pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork()"),
    pytest.mark.filterwarnings("ignore:.*multi-threaded.*fork:DeprecationWarning"),
]


class SlowJob(quebec.BaseClass):
    def perform(self, started: str, done: str, seconds: float) -> None:
        with open(started, "w") as fh:
            fh.write("x")
        time.sleep(seconds)
        with open(done, "w") as fh:
            fh.write("x")


def _make_quebec(temp_db_path, test_prefix, tmp_path, **kwargs):
    qc = quebec.Quebec(
        f"sqlite:///{temp_db_path}?mode=rwc",
        table_name_prefix=test_prefix,
        process_heartbeat_interval=0.1,
        shutdown_timeout=SHORT_SHUTDOWN,
        **kwargs,
    )
    assert qc.create_tables()
    qc.register_job(SlowJob)
    SlowJob.perform_later(
        qc, str(tmp_path / "started"), str(tmp_path / "done"), JOB_SECONDS
    )
    return qc


def _reap(pid: int) -> None:
    deadline = time.monotonic() + REAP_GRACE
    while time.monotonic() < deadline:
        try:
            waited, _status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return
        if waited == pid:
            return
        time.sleep(0.05)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)


def _kill_quietly(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def test_an_orphan_drains_on_the_orphan_budget(
    temp_db_path, test_prefix, tmp_path
) -> None:
    """The job outlives shutdown_timeout, so finishing proves the longer
    orphan budget was used."""
    qc = _make_quebec(
        temp_db_path,
        test_prefix,
        tmp_path,
        orphan_drain_timeout=JOB_SECONDS + 10,
    )
    started = tmp_path / "started"
    done = tmp_path / "done"
    worker_pid_file = tmp_path / "worker.pid"
    ready = tmp_path / "watching"

    # A stands in for the supervisor: it forks the worker, waits until the
    # worker has latched onto its ppid, then dies — orphaning it.
    stand_in = os.fork()
    if stand_in == 0:
        try:
            worker = os.fork()
            if worker == 0:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                qc.reset_after_fork()
                qc.watch_parent_pid()
                ready.write_text("x")
                qc.run(spawn=["worker"], create_tables=False)
                os._exit(0)
            worker_pid_file.write_text(str(worker))
            while not ready.exists():
                time.sleep(0.02)
            os._exit(0)
        except BaseException:  # noqa: BLE001 - must not unwind into pytest
            os._exit(1)

    _reap(stand_in)
    worker_pid = int(worker_pid_file.read_text())
    try:
        wait_until(
            started.exists, timeout=20, interval=0.05, message="the job never started"
        )
        # The supervisor is already gone; the worker notices and shuts down,
        # and the job has seconds left to run.
        wait_until(
            done.exists,
            timeout=JOB_SECONDS + 10,
            interval=0.05,
            message="the orphan cut its job off instead of draining",
        )
    finally:
        _kill_quietly(worker_pid)
        qc.close()


def test_a_supervised_worker_still_uses_the_short_budget(
    temp_db_path, test_prefix, tmp_path
) -> None:
    """The orphan budget must not leak into the ordinary SIGTERM path, where
    the supervisor is waiting to SIGKILL the remainder."""
    qc = _make_quebec(
        temp_db_path,
        test_prefix,
        tmp_path,
        orphan_drain_timeout=JOB_SECONDS + 10,
    )
    started = tmp_path / "started"
    done = tmp_path / "done"

    worker = os.fork()
    if worker == 0:
        try:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            qc.reset_after_fork()
            qc.run(spawn=["worker"], create_tables=False)
            os._exit(0)
        except BaseException:  # noqa: BLE001
            os._exit(1)

    try:
        wait_until(
            started.exists, timeout=20, interval=0.05, message="the job never started"
        )
        os.kill(worker, signal.SIGTERM)
        _reap(worker)

        # Exited on the short budget, well before the job could finish.
        assert not done.exists(), (
            "a supervised worker waited past shutdown_timeout for its job"
        )
    finally:
        _kill_quietly(worker)
        qc.close()

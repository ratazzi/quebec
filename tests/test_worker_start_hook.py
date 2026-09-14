"""`on_worker_start` has to actually run, and run in the right process.

The existing lifecycle tests only check that registration doesn't raise, so
nothing caught a hook that is registered and then never invoked. That matters
because applications hang process-scoped setup off this hook — acquiring a
per-process id, opening a dedicated connection — and under the fork supervisor
the point is that it runs *in each worker child*, after the fork, rather than
once in the parent.

Both tests run the worker in a forked child and stop it with SIGTERM:
`graceful_shutdown()` ends with `std::process::exit(0)`, so calling it in the
pytest process would take the test run down with it.
"""

from __future__ import annotations

import os
import signal
import time

import pytest
import quebec

from .helpers import wait_until

SHUTDOWN_GRACE = 15.0
pytestmark = [
    pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork()"),
    pytest.mark.filterwarnings("ignore:.*multi-threaded.*fork:DeprecationWarning"),
]


def _fork_worker(qc) -> int:
    """Run a worker in a child, as the fork supervisor does."""
    pid = os.fork()
    if pid == 0:
        try:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            qc.reset_after_fork()
            qc.run(spawn=["worker"], create_tables=False)
            os._exit(0)
        except BaseException:  # noqa: BLE001 - must not unwind into pytest
            os._exit(1)
    return pid


def _stop(pids: list[int]) -> None:
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    for pid in pids:
        deadline = time.monotonic() + SHUTDOWN_GRACE
        while time.monotonic() < deadline:
            waited, _status = os.waitpid(pid, os.WNOHANG)
            if waited == pid:
                break
            time.sleep(0.05)
        else:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)


@pytest.fixture
def marks(tmp_path):
    """Each process that runs the hook drops a file named after its pid."""
    directory = tmp_path / "started"
    directory.mkdir()
    return directory


def _quebec_with_hook(sqlite_url, marks):
    qc = quebec.Quebec(sqlite_url)
    assert qc.create_tables()

    # Registered before any fork, exactly as an application's main() would.
    @qc.on_worker_start
    def record() -> None:
        (marks / str(os.getpid())).write_text("started")

    return qc


def test_the_hook_runs_when_a_worker_starts(sqlite_url, marks) -> None:
    qc = _quebec_with_hook(sqlite_url, marks)
    child = _fork_worker(qc)
    try:
        wait_until(
            lambda: any(marks.iterdir()),
            timeout=20,
            interval=0.1,
            message="on_worker_start was registered but never invoked",
        )
    finally:
        _stop([child])
        qc.close()

    assert [int(p.name) for p in marks.iterdir()] == [child]


def test_each_forked_worker_runs_the_hook_in_its_own_process(
    sqlite_url, marks
) -> None:
    """Registered once in the parent, run once per child — never in the parent.

    An application that does this setup before the fork instead gets one shared
    result for the whole fleet, which is the bug this hook exists to avoid.
    """
    qc = _quebec_with_hook(sqlite_url, marks)
    parent_pid = os.getpid()
    children: list[int] = []
    try:
        children = [_fork_worker(qc) for _ in range(2)]
        wait_until(
            lambda: len(list(marks.iterdir())) == len(children),
            timeout=20,
            interval=0.1,
            message="not every forked worker ran the hook",
        )
    finally:
        _stop(children)
        qc.close()

    ran_in = {int(p.name) for p in marks.iterdir()}
    assert ran_in == set(children), "the hook ran in the wrong processes"
    assert parent_pid not in ran_in, "the hook must not run in the forking parent"

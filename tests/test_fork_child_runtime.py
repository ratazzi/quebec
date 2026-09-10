"""Regression test: a forked worker child must not crash on its own runtime.

The fork supervisor calls ``os.fork()`` while the parent still has a live
multi-threaded tokio runtime, then rebuilds the runtime in the child via
``reset_after_fork()``. With tokio's ``parking_lot`` feature enabled the
parent's idle runtime workers are blocked on a ``parking_lot::Condvar``, which
registers each of them in parking_lot's *process-global* hash table. The child
inherits that table verbatim, but the threads it points at do not exist there,
and glibc recycles their stacks and dynamic TLS as soon as the child creates
threads of its own. The child's first hash-table resize then walks the stale
queues and either dereferences freed memory (SIGSEGV) or writes through it and
smashes the heap ("malloc(): invalid size (unsorted)", SIGABRT).

Reproduced on x86_64 Linux (glibc 2.35): 17 of 30 supervisor starts crashed a
child with the feature on, 0 of 30 with it off. arm64 hosts never showed it.
The child has to actually run its worker loop for about a second to get there
-- just calling ``reset_after_fork()`` and exiting is not enough to trigger the
resize.

Because that makes the crash host- and timing-dependent, this module covers the
bug twice: once by forking real workers, and once by pinning the build
configuration the fix rests on.
"""

from __future__ import annotations

import os
import re
import signal
import threading
import time
from pathlib import Path

import pytest
import quebec

CARGO_LOCK = Path(__file__).resolve().parent.parent / "Cargo.lock"

# A handful of rounds; each costs about a second of child lifetime.
FORK_ROUNDS = 6
CHILD_LIFETIME = 1.0
SHUTDOWN_GRACE = 5.0
FATAL_SIGNALS = {signal.SIGSEGV, signal.SIGABRT, signal.SIGBUS, signal.SIGILL}
# Roughly 400 KB/s, in the range of a log stream going to a terminal.
DRAIN_CHUNK = 8192
DRAIN_INTERVAL = 0.02


def _drain_slowly(fd: int) -> None:
    """Read ``fd`` at roughly terminal speed until every writer is gone."""
    while True:
        try:
            if not os.read(fd, DRAIN_CHUNK):
                return
        except OSError:
            return
        time.sleep(DRAIN_INTERVAL)


def _reap(pid: int) -> int:
    """Wait for ``pid`` to exit, SIGKILLing it if it wedges. Returns wait status.

    A child that misses its SIGTERM is not reported as a failure: it is most
    likely blocked writing to the throttled pipe below, which is our doing
    rather than a bug.
    """
    deadline = time.monotonic() + SHUTDOWN_GRACE
    while time.monotonic() < deadline:
        waited, status = os.waitpid(pid, os.WNOHANG)
        if waited == pid:
            return status
        time.sleep(0.05)
    os.kill(pid, signal.SIGKILL)
    _, status = os.waitpid(pid, 0)
    return status


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork()")
@pytest.mark.filterwarnings("ignore:.*multi-threaded.*fork:DeprecationWarning")
def test_forked_worker_child_does_not_crash(sqlite_url):
    qc = quebec.Quebec(sqlite_url)
    assert qc.create_tables()

    # Give the children a pipe for their log output. Under pytest's default fd
    # capture their stderr would be a regular file, and the crash reproduced
    # far less often that way -- writing to a pipe is slower and shifts the
    # startup timing the race depends on. The reader is throttled to something
    # like terminal speed but must keep moving: a pipe left permanently full
    # stalls each child on its first log line, before it builds a runtime.
    read_fd, write_fd = os.pipe()
    drain = threading.Thread(target=_drain_slowly, args=(read_fd,), daemon=True)
    drain.start()
    try:
        _run_fork_rounds(qc, write_fd)
    finally:
        os.close(write_fd)
        drain.join(timeout=SHUTDOWN_GRACE)
        os.close(read_fd)


def _run_fork_rounds(qc, write_fd: int) -> None:
    for i in range(FORK_ROUNDS):
        pid = os.fork()
        if pid == 0:
            try:
                os.dup2(write_fd, 1)
                os.dup2(write_fd, 2)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                qc.reset_after_fork()
                qc.watch_parent_pid()
                qc.run(spawn=["worker"], create_tables=False)
                os._exit(0)
            except BaseException:  # noqa: BLE001 - must not unwind into pytest
                os._exit(1)

        time.sleep(CHILD_LIFETIME)
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        status = _reap(pid)

        if os.WIFSIGNALED(status) and os.WTERMSIG(status) in FATAL_SIGNALS:
            pytest.fail(
                f"forked worker died from signal {os.WTERMSIG(status)} on round {i}"
            )


def test_tokio_is_not_built_with_parking_lot():
    """Pin the fix itself: tokio must not pull in parking_lot.

    Cargo.lock records the union of every enabled optional dependency, so
    ``parking_lot`` shows up under tokio's dependency list exactly when some
    crate in the graph turns the feature on -- including indirectly, which a
    check against Cargo.toml alone would miss. The runtime test above only
    catches this on the hosts where the crash actually reproduces; this one
    catches it everywhere.
    """
    if not CARGO_LOCK.exists():
        pytest.skip("not running from a source checkout")

    block = re.search(
        r'^\[\[package\]\]\nname = "tokio"\n.*?(?=^\[\[package\]\]|\Z)',
        CARGO_LOCK.read_text(),
        re.MULTILINE | re.DOTALL,
    )
    assert block is not None, "tokio not found in Cargo.lock"
    assert '"parking_lot"' not in block.group(0), (
        "tokio is built with the parking_lot feature again; see the module "
        "docstring -- its condvar parking is not fork-safe"
    )

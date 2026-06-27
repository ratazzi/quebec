"""systemd sd_notify integration (READY / STATUS / WATCHDOG / STOPPING).

The protocol lives in Rust (src/systemd.rs) and is exposed via the
``systemd_ready(status)`` / ``systemd_notify(status)`` / ``systemd_stop()``
methods — the status line is built from in-process state by the caller. Here
we bind a real ``AF_UNIX`` datagram socket, point ``NOTIFY_SOCKET`` at it, and
assert on the datagrams Quebec emits — the same wire protocol systemd speaks.

Environment mutations go through ``monkeypatch`` so a stray ``NOTIFY_SOCKET`` /
``WATCHDOG_*`` from the surrounding shell (e.g. running under a systemd service)
can't leak into or out of these tests.
"""

import os
import shutil
import socket
import tempfile

import pytest


@pytest.fixture
def notify_socket(monkeypatch):
    """A bound AF_UNIX/SOCK_DGRAM socket with NOTIFY_SOCKET pointed at it.

    Uses a short /tmp path to stay under the ~104 char sun_path limit on macOS;
    monkeypatch restores NOTIFY_SOCKET afterwards.
    """
    tmpdir = tempfile.mkdtemp(prefix="qbsd", dir="/tmp")
    sock_path = os.path.join(tmpdir, "n")
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    srv.bind(sock_path)
    monkeypatch.setenv("NOTIFY_SOCKET", sock_path)
    try:
        yield srv
    finally:
        srv.close()
        shutil.rmtree(tmpdir, ignore_errors=True)


def _drain(srv, timeout=5.0):
    """Receive one datagram, decoded, raising on timeout."""
    srv.settimeout(timeout)
    return srv.recv(8192).decode()


def test_noop_without_notify_socket(qc, monkeypatch):
    """Without NOTIFY_SOCKET the calls are silent no-ops, never raising."""
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    qc.systemd_ready("Quebec test: running")
    qc.systemd_notify("Quebec test: running")
    qc.systemd_stop()


def test_ready_and_status(qc, notify_socket):
    qc.systemd_ready("Quebec test: workers 2/2; running")
    msg = _drain(notify_socket)
    assert "READY=1" in msg
    assert "STATUS=Quebec test: workers 2/2; running" in msg


def test_stopping(qc, notify_socket):
    qc.systemd_stop()
    msg = _drain(notify_socket)
    assert "STOPPING=1" in msg


def test_notify_refreshes_status(qc, notify_socket, monkeypatch):
    """Without a watchdog, notify sends STATUS but not WATCHDOG."""
    monkeypatch.delenv("WATCHDOG_USEC", raising=False)
    qc.systemd_notify("Quebec test: workers 1/1; running")
    msg = _drain(notify_socket)
    assert "STATUS=Quebec test: workers 1/1; running" in msg
    assert "WATCHDOG=1" not in msg


def test_watchdog_petted_on_notify(qc, notify_socket, monkeypatch):
    """With WATCHDOG_USEC set, each notify also pets the watchdog."""
    monkeypatch.setenv("WATCHDOG_USEC", "2000000")
    # watchdog_enabled() honours WATCHDOG_PID; pin it to this process so an
    # inherited value pointing elsewhere can't suppress the watchdog.
    monkeypatch.setenv("WATCHDOG_PID", str(os.getpid()))
    qc.systemd_notify("Quebec test: workers 1/1; running")
    msg = _drain(notify_socket)
    assert "WATCHDOG=1" in msg
    assert "STATUS=" in msg


def test_single_process_status_reports_worker_idle(qc):
    """The single-process STATUS line reports live worker thread/idle figures
    from the worker-owned claim ledger — process-local, no DB query."""
    from quebec import _systemd_status_line

    assert qc.in_flight_count() == 0  # nothing claimed/running in a fresh instance
    line = _systemd_status_line(qc, ["worker", "dispatcher", "scheduler"])
    # Idle = total threads when nothing is running.
    assert f"worker {qc.worker_threads} threads, {qc.worker_threads} idle" in line
    assert "dispatcher" in line
    assert "scheduler" in line
    assert line.startswith("Quebec ")

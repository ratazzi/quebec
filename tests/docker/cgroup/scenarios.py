#!/usr/bin/env python3
"""cgroup v2 integration scenarios, run inside a Linux container.

Each scenario runs in its own process (``scenarios.py run <name>``) because
``Supervisor.start()`` blocks, installs signal handlers on the main thread, and
leaves the Quebec instance unusable afterwards. With no arguments this file is
the driver: it re-executes itself once per scenario and reports a summary.

Exit code is non-zero if any scenario fails.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import tempfile
import threading
import time

import quebec
from quebec import cgroup as cgroup_mod
from quebec.supervisor import ROLE_WORKER, Supervisor

SCENARIOS = ["oom", "no_limits", "cleanup", "derive"]

MIB = 1024 * 1024


class Failure(AssertionError):
    pass


def check(condition, message):
    if not condition:
        raise Failure(message)
    print(f"    ok: {message}")


class LogCapture(logging.Handler):
    """Collects supervisor log records so scenarios can assert on them."""

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())

    @property
    def text(self):
        return "\n".join(self.lines)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    capture = LogCapture()
    logging.getLogger("quebec").addHandler(capture)
    return capture


def write_queue_yml(body: str) -> str:
    path = os.path.join(tempfile.mkdtemp(), "queue.yml")
    with open(path, "w") as fh:
        fh.write(body)
    os.environ["QUEBEC_CONFIG"] = path
    os.environ["QUEBEC_ENV"] = "test"
    return path


def make_quebec(**kwargs):
    db_path = os.path.join(tempfile.mkdtemp(), "quebec.db")
    qc = quebec.Quebec(f"sqlite:///{db_path}?mode=rwc", **kwargs)
    assert qc.create_tables() is True
    return qc, db_path


def failed_rows(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(
            "SELECT job_id, error FROM solid_queue_failed_executions"
        ).fetchall()
    finally:
        conn.close()


def run_supervisor(sup, until, timeout=60.0):
    """Run the supervisor until ``until()`` is true, then stop it."""
    stop = threading.Event()

    def watcher():
        deadline = time.monotonic() + timeout
        while not stop.wait(0.2):
            try:
                if until():
                    break
            except Exception as exc:
                # The predicate races the supervisor: cgroup files and DB rows
                # appear and vanish under it. Keep polling.
                print(f"    (poll: {type(exc).__name__}: {exc})")
            if time.monotonic() > deadline:
                print("    (watchdog timeout, stopping supervisor)")
                break
        # Give the supervisor a beat to finish its own bookkeeping for the
        # exit we just observed before asking it to shut down.
        time.sleep(1.0)
        sup.stop()

    thread = threading.Thread(target=watcher, daemon=True)
    thread.start()
    try:
        sup.start()
    finally:
        stop.set()
        thread.join(timeout=5)


def cgroup_root():
    probe = cgroup_mod.probe()
    return probe.root if probe.enabled else None


class HogJob(quebec.ActiveJob):
    """Allocates far past the limit, touching every page so it is resident."""

    def perform(self, *args, **kwargs):
        chunks = []
        for _ in range(512):
            block = bytearray(1 * MIB)
            for offset in range(0, len(block), 4096):
                block[offset] = 1
            chunks.append(block)
        return len(chunks)


class IdleJob(quebec.ActiveJob):
    def perform(self, *args, **kwargs):
        return True


def scenario_oom():
    """A job over memory_max kills only its worker, and is attributed as OOM."""
    write_queue_yml(
        """
test:
  workers:
    - queues: "*"
      threads: 1
      processes: 1
      memory_max: 128MiB
"""
    )
    capture = setup_logging()
    qc, db_path = make_quebec()
    qc.register_job(HogJob)
    HogJob.perform_later(qc)

    sup = Supervisor(qc, {ROLE_WORKER: 1}, shutdown_timeout=2.0, crash_loop_max=99)
    root = cgroup_root()
    check(root is not None, f"cgroup probe succeeded (root={root})")

    run_supervisor(sup, lambda: len(failed_rows(db_path)) > 0)

    rows = failed_rows(db_path)
    check(len(rows) == 1, f"exactly one failed execution recorded (got {len(rows)})")
    error = rows[0][1] or ""
    print(f"    failed_executions.error = {error}")
    check(
        "killed by the OOM killer" in error,
        "error states the fact the counters prove",
    )
    check("memory.peak=" in error, "error carries memory.peak")
    check("memory.max=134217728" in error, "error carries the configured memory.max")
    check("oom_kill=" in error, "error carries the oom_kill counter")
    check(
        "Likely exceeded this worker's memory.max" in error,
        "hitting our own memory.max is called out",
    )

    text = capture.text
    check(
        "was killed by the OOM killer" in text,
        "supervisor classified the exit as an OOM",
    )
    # P1-1: the refork must land in a directory of its own, never reuse one
    # that still holds the previous child.
    check(
        "is still occupied by another process" not in text,
        "a clean refork does not have to step over an occupied cgroup",
    )
    check(
        text.count("Forked worker[0] as pid=") >= 2,
        "the slot was reforked after the OOM",
    )
    qc.close()


def scenario_no_limits():
    """With nothing configured the cgroup is created but left unconstrained."""
    write_queue_yml(
        """
test:
  workers:
    - queues: "*"
      threads: 1
      processes: 1
"""
    )
    setup_logging()
    qc, db_path = make_quebec()
    qc.register_job(IdleJob)
    IdleJob.perform_later(qc)

    root = cgroup_root()
    check(root is not None, f"cgroup probe succeeded (root={root})")
    slot_dir = os.path.join(root, "worker-0")
    observed = {}

    def worker_is_up():
        if os.path.isdir(slot_dir):
            with open(os.path.join(slot_dir, "memory.max")) as fh:
                observed["memory.max"] = fh.read().strip()
            with open(os.path.join(slot_dir, "cgroup.procs")) as fh:
                observed["procs"] = fh.read().split()
            return bool(observed["procs"])
        return False

    sup = Supervisor(qc, {ROLE_WORKER: 1}, shutdown_timeout=2.0)
    run_supervisor(sup, worker_is_up, timeout=20.0)

    check("memory.max" in observed, "the worker cgroup was created")
    print(f"    worker-0/memory.max = {observed.get('memory.max')}")
    check(
        observed.get("memory.max") == "max",
        "no limit configured means memory.max stays at 'max'",
    )
    check(len(observed.get("procs", [])) == 1, "the worker was migrated into it")
    check(failed_rows(db_path) == [], "no job failed")
    qc.close()


def scenario_cleanup():
    """Slot cgroups are gone once the supervisor has shut down."""
    write_queue_yml(
        """
test:
  workers:
    - queues: "*"
      threads: 1
      processes: 1
      memory_max: 256MiB
"""
    )
    setup_logging()
    qc, _db_path = make_quebec()
    qc.register_job(IdleJob)

    root = cgroup_root()
    check(root is not None, f"cgroup probe succeeded (root={root})")
    slot_dir = os.path.join(root, "worker-0")
    seen = {"created": False}

    def worker_is_up():
        if os.path.isdir(slot_dir):
            seen["created"] = True
            return True
        return False

    sup = Supervisor(qc, {ROLE_WORKER: 1}, shutdown_timeout=2.0)
    run_supervisor(sup, worker_is_up, timeout=20.0)

    check(seen["created"], "the worker cgroup existed while running")
    check(
        not any(p.startswith("worker-0.") for p in os.listdir(root)),
        "no orphaned suffixed cgroup directories were left behind",
    )
    check(not os.path.isdir(slot_dir), f"{slot_dir} was removed on shutdown")
    check(
        os.path.isdir(os.path.join(root, "supervisor")),
        "the supervisor's own leaf is intentionally kept for reuse",
    )
    qc.close()


def scenario_derive():
    """memory.max is derived from memory_recycle_at when not set explicitly."""
    write_queue_yml(
        """
test:
  workers:
    - queues: "*"
      threads: 1
      processes: 1
      memory_recycle_at: 100MiB
"""
    )
    capture = setup_logging()
    qc, _db_path = make_quebec()
    qc.register_job(IdleJob)

    root = cgroup_root()
    check(root is not None, f"cgroup probe succeeded (root={root})")
    slot_dir = os.path.join(root, "worker-0")
    observed = {}

    def worker_is_up():
        if os.path.isdir(slot_dir):
            with open(os.path.join(slot_dir, "memory.max")) as fh:
                observed["memory.max"] = fh.read().strip()
            with open(os.path.join(slot_dir, "memory.swap.max")) as fh:
                observed["memory.swap.max"] = fh.read().strip()
            with open(os.path.join(slot_dir, "memory.oom.group")) as fh:
                observed["memory.oom.group"] = fh.read().strip()
            return True
        return False

    sup = Supervisor(qc, {ROLE_WORKER: 1}, shutdown_timeout=2.0)
    run_supervisor(sup, worker_is_up, timeout=20.0)

    print(f"    observed = {observed}")
    check(
        observed.get("memory.max") == str(150 * MIB),
        f"100MiB x1.5 -> 150MiB (got {observed.get('memory.max')})",
    )
    check(observed.get("memory.swap.max") == "0", "swap defaults to 0")
    check(observed.get("memory.oom.group") == "1", "oom.group defaults to 1")
    check(
        "derived from memory_recycle_at=100MiB -> 150MiB" in capture.text,
        "startup logged the derived value",
    )
    qc.close()


USAGE = f"""usage: scenarios.py {{run <name>|probe|supervisor-boot}}

Scenarios must run one per container: the supervisor has to be the only
process in the container's cgroup, or the kernel refuses to enable
controllers there ("no internal process" rule). tests/docker/cgroup/run.sh
drives one `docker run` per scenario.

available scenarios: {", ".join(SCENARIOS)}
"""


def main():
    if len(sys.argv) == 1:
        print(USAGE)
        return 2
    if sys.argv[1] == "probe":
        # Used by the degradation checks: report whether cgroups are usable.
        print(cgroup_mod.probe())
        return 0
    if sys.argv[1] == "supervisor-boot":
        # Used by the degradation checks: build a Supervisor and report whether
        # construction succeeded. queue.yml comes from the caller's env.
        setup_logging()
        qc, _ = make_quebec()
        qc.register_job(IdleJob)
        try:
            Supervisor(qc, {ROLE_WORKER: 1})
        except RuntimeError as exc:
            print(f"STARTUP_ERROR: {exc}")
            return 3
        print("SUPERVISOR_CONSTRUCTED")
        return 0
    if sys.argv[1] != "run":
        print(f"unknown command {sys.argv[1]!r}")
        return 2

    name = sys.argv[2]
    fn = globals()[f"scenario_{name}"]
    try:
        fn()
    except Failure as exc:
        print(f"FAIL: {exc}")
        return 1
    print(f"PASS: {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

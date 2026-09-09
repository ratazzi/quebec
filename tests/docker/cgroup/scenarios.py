#!/usr/bin/env python3
"""cgroup v2 integration scenarios, run inside a Linux container.

Each scenario runs in its own process (``scenarios.py run <name>``) because
``Supervisor.start()`` blocks, installs signal handlers on the main thread, and
leaves the Quebec instance unusable afterwards. With no arguments this file is
the driver: it re-executes itself once per scenario and reports a summary.

Exit code is non-zero if any scenario fails.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

import quebec
from quebec import cgroup as cgroup_mod
from quebec.supervisor import ROLE_WORKER, Supervisor

SCENARIOS = [
    "oom",
    "no_limits",
    "cleanup",
    "derive",
    "observe",
    "observe_readonly",
    "rolling_restart",
    "placement_failure",
]

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


def worker_metadata(db_path):
    """Latest Worker row metadata, parsed. Empty dict until a heartbeat lands."""
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT metadata FROM solid_queue_processes "
            "WHERE kind = 'Worker' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        return {}
    try:
        return json.loads(row[0])
    except ValueError:
        return {}


def fetch(url, timeout=5.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


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


class SlowJob(quebec.ActiveJob):
    """Long enough that the second supervisor starts while it is still running."""

    def perform(self, seconds=15, *args, **kwargs):
        time.sleep(seconds)
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


def _observe(*, with_limit: bool):
    """Shared body for the two observability scenarios.

    Phase 2 metrics are read-only, so they must show up even where the
    hierarchy is not writable and no limits can be configured at all.
    """
    write_queue_yml(
        """
test:
  workers:
    - queues: "*"
      threads: 1
      processes: 1
"""
        + ("      memory_max: 256MiB\n" if with_limit else "")
    )
    setup_logging()
    # The default 60s heartbeat is the only publisher of cgroup metrics;
    # shorten it so the scenario does not have to wait a minute.
    qc, db_path = make_quebec(process_heartbeat_interval=2)
    qc.register_job(IdleJob)

    probe = cgroup_mod.probe()
    print(f"    probe = {probe}")
    check(
        probe.enabled is with_limit,
        f"cgroup writability matches the run mode (enabled={probe.enabled})",
    )

    seen = {}

    def metrics_published():
        meta = worker_metadata(db_path)
        if "cgroup_current_bytes" in meta:
            seen.update(meta)
            return True
        return False

    sup = Supervisor(
        qc,
        {ROLE_WORKER: 1},
        shutdown_timeout=2.0,
        control_plane="127.0.0.1:5006",
    )

    page = {}

    def until():
        if not metrics_published():
            return False
        try:
            page["html"] = fetch("http://127.0.0.1:5006/workers")
        except (urllib.error.URLError, OSError) as exc:
            print(f"    (control plane not ready yet: {exc})")
            return False
        return True

    # Generous: metrics only reach the DB on a heartbeat, and a loaded host
    # can push the worker's first heartbeat out by several intervals.
    run_supervisor(sup, until, timeout=90.0)

    print(
        f"    metadata = { {k: v for k, v in seen.items() if k.startswith(('cgroup', 'cpu'))} }"
    )
    if "cgroup_current_bytes" not in seen:
        # Separate "the kernel will not show us the files" from "the sampler
        # did not run": both surface as empty metadata.
        print(f"    raw metadata row = {worker_metadata(db_path)!r}")
        for probe_path in (
            "/proc/self/cgroup",
            "/proc/mounts",
            "/sys/fs/cgroup/memory.current",
            "/sys/fs/cgroup/cpu.stat",
        ):
            try:
                with open(probe_path) as fh:
                    body = fh.read()
                if probe_path == "/proc/mounts":
                    body = "\n".join(ln for ln in body.splitlines() if "cgroup" in ln)
                print(f"    {probe_path}: {body.strip()[:200]!r}")
            except OSError as exc:
                print(f"    {probe_path}: UNREADABLE {exc}")
    check("cgroup_current_bytes" in seen, "metadata carries cgroup_current_bytes")
    check(seen["cgroup_current_bytes"] > 0, "memory.current is a real reading")
    check("cpu_usage_usec" in seen, "metadata carries cpu.stat usage_usec")

    if with_limit:
        check(
            seen.get("cgroup_memory_max") == 256 * MIB,
            f"memory.max reported as 256MiB (got {seen.get('cgroup_memory_max')})",
        )
        check(
            seen["cgroup_current_bytes"] <= seen["cgroup_memory_max"],
            "memory.current stays within memory.max",
        )
    else:
        check(
            "cgroup_memory_max" not in seen,
            "an unlimited cgroup reports no memory.max",
        )

    html = page.get("html", "")
    check(bool(html), "control plane /workers responded")
    check("cgroup" in html, "the workers page renders a cgroup column")
    if with_limit:
        check('role="progressbar"' in html, "the page renders a memory progress bar")
        check("% of memory.max" in html, "the bar is labelled against memory.max")
    else:
        check("unlimited" in html, "an unlimited cgroup renders without a bar")
    qc.close()


def scenario_observe():
    """Privileged: metrics plus a limit, so the bar has something to divide by."""
    _observe(with_limit=True)


def scenario_observe_readonly():
    """Unprivileged, read-only /sys/fs/cgroup: metrics must still be readable."""
    _observe(with_limit=False)


def scenario_placement_failure():
    """A real fork must be reaped when initial cgroup placement fails."""
    setup_logging()
    root = cgroup_root()
    check(root is not None, "cgroup probe succeeded")

    class BadPlace(cgroup_mod.CgroupManager):
        def place(self, pid, role, index):
            raise cgroup_mod.CgroupError("injected initial placement failure")

    qc, _db_path = make_quebec()
    sup = Supervisor(
        qc,
        {ROLE_WORKER: 1},
        cgroup=BadPlace(root),
        limits={ROLE_WORKER: [{"memory_max": 128 * MIB}]},
    )
    try:
        try:
            sup.start()
        except cgroup_mod.CgroupError as exc:
            check("injected initial placement failure" in str(exc), "startup failed")
        else:
            raise Failure("startup succeeded despite placement failure")
        check(not sup._children, "the aborted child was reaped")
        try:
            os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            check(True, "no child process or zombie remains")
        else:
            raise Failure("a child escaped startup cleanup")
        check(not slot_dirs(root), "the aborted child's cgroup was removed")
    finally:
        qc.close()


SUPERVISOR_BOOT = """
import os, sys, time, quebec
from quebec.supervisor import ROLE_WORKER, Supervisor

db_path, marker = sys.argv[1], sys.argv[2]
# shutdown_timeout is the *worker's* drain budget; the default 5s would cut
# the slow job short, which is exactly what this scenario must not do.
qc = quebec.Quebec(
    "sqlite:///%s?mode=rwc" % db_path,
    process_heartbeat_interval=2,
    shutdown_timeout=45,
)

class SlowJob(quebec.ActiveJob):
    def perform(self, seconds=15, *args, **kwargs):
        time.sleep(seconds)
        return True

class IdleJob(quebec.ActiveJob):
    def perform(self, *args, **kwargs):
        return True

qc.register_job(SlowJob)
qc.register_job(IdleJob)
with open(marker, "w") as fh:
    fh.write(str(os.getpid()))
Supervisor(qc, {ROLE_WORKER: 1}, shutdown_timeout=30.0).start()
"""


def spawn_supervisor(db_path, marker, cgroup_root):
    """Start a supervisor in its own session so our signals do not reach it."""
    env = dict(os.environ, QUEBEC_CGROUP_ROOT=cgroup_root)
    return subprocess.Popen(
        [sys.executable, "-c", SUPERVISOR_BOOT, db_path, marker],
        env=env,
        start_new_session=True,
    )


def wait_for(predicate, timeout, what):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.25)
    print(f"    (timed out waiting for {what})")
    return False


def read_text(*parts):
    with open(os.path.join(*parts)) as fh:
        return fh.read()


def slot_dirs(root, prefix="worker-0"):
    return sorted(
        name
        for name in os.listdir(root)
        if name == prefix or name.startswith(prefix + ".")
    )


def claimed_by(db_path, pids):
    """Wait on database ownership, not merely the worker's cgroup placement."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT p.pid FROM solid_queue_claimed_executions c "
            "JOIN solid_queue_processes p ON p.id = c.process_id"
        ).fetchall()
        return any(pid in pids for (pid,) in rows)
    finally:
        conn.close()


def scenario_rolling_restart():
    """Two supervisor generations must never share a worker cgroup.

    Reproduces the real hazard: A's worker is still draining a job when B
    starts. If B claimed the same directory it would rewrite A's limits, and
    whichever generation exited first would rmdir/kill the other's child.
    """
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
    root = cgroup_root()
    check(root is not None, f"cgroup probe succeeded (root={root})")

    # The supervisors need the root to have no member processes before they
    # can enable controllers there, so this driver vacates it first.
    driver_leaf = os.path.join(root, "driver")
    os.makedirs(driver_leaf, exist_ok=True)
    with open(os.path.join(driver_leaf, "cgroup.procs"), "w") as fh:
        fh.write(str(os.getpid()))

    # Exercise the create/place gap against real cgroupfs, before starting
    # the workers. A live owner protects even an empty, unplaced cgroup.
    owner_a = cgroup_mod.CgroupManager(root)
    owner_b = cgroup_mod.CgroupManager(root)
    owner_a.prepare()
    try:
        path_a = owner_a.create(ROLE_WORKER, 0, cgroup_mod.Limits())
        owner_b.scavenge()
        path_b = owner_b.create(ROLE_WORKER, 0, cgroup_mod.Limits())
        check(path_a != path_b, "scavenging preserved the unplaced child cgroup")
    finally:
        owner_a.destroy(ROLE_WORKER, 0)
        owner_b.destroy(ROLE_WORKER, 0)

    qc, db_path = make_quebec()
    qc.register_job(SlowJob)
    SlowJob.perform_later(qc, 15)
    qc.close()

    tmp = tempfile.mkdtemp()
    marker_a = os.path.join(tmp, "a.pid")
    marker_b = os.path.join(tmp, "b.pid")

    proc_a = spawn_supervisor(db_path, marker_a, root)
    check(
        wait_for(lambda: slot_dirs(root) == ["worker-0"], 60.0, "A's worker cgroup"),
        "supervisor A claimed worker-0",
    )
    check(
        wait_for(
            lambda: bool(read_text(root, "worker-0", "cgroup.procs").split()),
            60.0,
            "A's worker to be placed",
        ),
        "supervisor A's worker is in worker-0",
    )
    a_limit = read_text(root, "worker-0", "memory.max").strip()
    print(f"    A worker-0/memory.max = {a_limit}")
    a_pids = {int(pid) for pid in read_text(root, "worker-0", "cgroup.procs").split()}
    check(
        wait_for(lambda: claimed_by(db_path, a_pids), 60.0, "A to claim the slow job"),
        "A owns the slow job before B starts",
    )

    proc_b = spawn_supervisor(db_path, marker_b, root)
    check(
        wait_for(lambda: len(slot_dirs(root)) >= 2, 60.0, "B's worker cgroup"),
        "supervisor B created a second cgroup",
    )
    dirs = slot_dirs(root)
    print(f"    slot dirs = {dirs}")
    check(
        dirs[1].startswith("worker-0."),
        f"B claimed a suffixed directory, not worker-0 (got {dirs})",
    )
    check(
        read_text(root, "worker-0", "memory.max").strip() == a_limit,
        "A's memory.max was not rewritten by B",
    )
    b_dir = os.path.join(root, dirs[1])
    check(
        wait_for(
            lambda: bool(read_text(b_dir, "cgroup.procs").split()),
            60.0,
            "B's worker to be placed",
        ),
        "B's worker is placed before A shuts down",
    )

    # A drains and exits; its slow job must still finish.
    proc_a.send_signal(signal.SIGTERM)
    proc_a.wait(timeout=60)
    check(True, "supervisor A exited after draining")
    check(
        wait_for(
            lambda: not os.path.isdir(os.path.join(root, "worker-0")), 30.0, "A cleanup"
        ),
        "A removed its own worker-0 on the way out",
    )
    check(os.path.isdir(b_dir), f"B's cgroup {dirs[1]} survived A's shutdown")
    check(proc_b.poll() is None, "supervisor B is still running")

    conn = sqlite3.connect(db_path)
    try:
        finished = conn.execute(
            "SELECT COUNT(*) FROM solid_queue_jobs WHERE finished_at IS NOT NULL"
        ).fetchone()[0]
        failed = conn.execute(
            "SELECT COUNT(*) FROM solid_queue_failed_executions"
        ).fetchone()[0]
    finally:
        conn.close()
    print(f"    finished={finished} failed={failed}")
    check(failed == 0, "the slow job was not failed by the restart")
    check(finished == 1, "the slow job ran to completion")

    proc_b.send_signal(signal.SIGTERM)
    proc_b.wait(timeout=60)
    check(
        wait_for(lambda: slot_dirs(root) == [], 30.0, "B cleanup"),
        f"both generations' cgroups are gone (left: {slot_dirs(root)})",
    )


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

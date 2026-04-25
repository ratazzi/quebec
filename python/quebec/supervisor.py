"""Fork-based multi-process supervisor for Quebec.

Mirrors the Solid Queue supervisor model: a parent process forks one child per
role-instance (worker / dispatcher / scheduler), monitors them via waitpid,
restarts crashes, and on shutdown sends SIGTERM then SIGKILL after a timeout.
"""

from __future__ import annotations

import logging
import os
import select
import signal
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

ROLE_WORKER = "worker"
ROLE_DISPATCHER = "dispatcher"
ROLE_SCHEDULER = "scheduler"
VALID_ROLES = {ROLE_WORKER, ROLE_DISPATCHER, ROLE_SCHEDULER}

Plan = Dict[str, Union[int, List]]


@dataclass
class _ChildInfo:
    role: str
    index: int
    pid: int


@dataclass
class _SlotState:
    """Restart history for a (role, index) slot."""

    spawn_times: List[float] = field(default_factory=list)
    disabled: bool = False


class Supervisor:
    """Fork-based supervisor.

    Args:
        qc: The Quebec instance whose config/registries the children inherit.
        plan: Mapping of role -> child count. Example:
            ``{"worker": 2, "dispatcher": 1, "scheduler": 1}``.
            The role keys must be one of ``worker``, ``dispatcher``, ``scheduler``.
        control_plane: If set, the HTTP listen address (e.g. ``"127.0.0.1:5006"``).
            The control plane runs only in the supervisor process.
        shutdown_timeout: Seconds to wait for children to exit after SIGTERM
            before escalating to SIGKILL.
        crash_loop_window / crash_loop_max: A slot that crashes more than
            ``crash_loop_max`` times in ``crash_loop_window`` seconds is
            considered unhealthy; auto-restart is disabled for that slot.
        heartbeat_interval: Seconds between supervisor heartbeats to the DB.
    """

    def __init__(
        self,
        qc,
        plan: Plan,
        *,
        control_plane: Optional[str] = None,
        shutdown_timeout: float = 30.0,
        crash_loop_window: float = 10.0,
        crash_loop_max: int = 3,
        heartbeat_interval: float = 60.0,
        maintenance_interval: float = 300.0,
    ):
        normalized: Dict[str, int] = {}
        for role, spec in plan.items():
            if role not in VALID_ROLES:
                raise ValueError(
                    f"unknown role {role!r}; must be one of {sorted(VALID_ROLES)}"
                )
            count = spec if isinstance(spec, int) else len(spec)
            if count <= 0:
                continue
            normalized[role] = count
        if not normalized:
            raise ValueError("plan must spawn at least one child process")

        self.qc = qc
        self.plan = normalized
        self.control_plane = control_plane
        self.shutdown_timeout = shutdown_timeout
        self.crash_loop_window = crash_loop_window
        self.crash_loop_max = crash_loop_max
        self.heartbeat_interval = heartbeat_interval
        self.maintenance_interval = maintenance_interval

        self._process_id: Optional[int] = None
        self._children: Dict[int, _ChildInfo] = {}
        self._slots: Dict[Tuple[str, int], _SlotState] = {}
        self._stopping = False
        self._hostname = socket.gethostname()
        self._wakeup_r, self._wakeup_w = os.pipe()
        os.set_blocking(self._wakeup_r, False)
        os.set_blocking(self._wakeup_w, False)
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop = threading.Event()
        self._maintenance_thread: Optional[threading.Thread] = None
        self._maintenance_stop = threading.Event()

    # -- public -----------------------------------------------------------

    def start(self) -> None:
        """Blocking entrypoint: register, fork children, supervise, cleanup."""
        self._process_id = self.qc.register_supervisor()
        logger.info(
            "Supervisor registered (process_id=%d, pid=%d)",
            self._process_id,
            os.getpid(),
        )
        self._install_signal_handlers()

        try:
            for role, count in self.plan.items():
                for index in range(count):
                    self._fork_child(role, index)

            # Start the control plane only after all children are forked so
            # the listening socket/runtime task stays parent-only. Children
            # would otherwise inherit the TcpListener fd.
            if self.control_plane:
                try:
                    self.qc.start_control_plane(self.control_plane)
                except Exception:
                    logger.exception("Failed to start control plane")

            self._start_heartbeat()
            self._start_maintenance()
            self._supervise()
        finally:
            try:
                self._terminate_gracefully()
            finally:
                self._heartbeat_stop.set()
                self._maintenance_stop.set()
                if self._heartbeat_thread is not None:
                    self._heartbeat_thread.join(timeout=2.0)
                if self._maintenance_thread is not None:
                    self._maintenance_thread.join(timeout=2.0)
                if self._process_id is not None:
                    try:
                        self.qc.deregister_process(self._process_id)
                    except Exception:
                        logger.exception("Failed to deregister supervisor row")

    def stop(self) -> None:
        """Request a graceful shutdown from another thread."""
        self._stopping = True
        self._wake()

    # -- internals --------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        def handler(signum, _frame):
            logger.info("Supervisor received signal %d", signum)
            self._stopping = True
            self._wake()

        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT):
            signal.signal(sig, handler)

    def _wake(self) -> None:
        try:
            os.write(self._wakeup_w, b"x")
        except BlockingIOError:
            pass
        except OSError:
            pass

    def _start_heartbeat(self) -> None:
        def loop():
            while not self._heartbeat_stop.wait(self.heartbeat_interval):
                try:
                    self.qc.heartbeat_process(self._process_id)
                except Exception as e:
                    logger.warning("Supervisor heartbeat failed: %s", e)

        self._heartbeat_thread = threading.Thread(
            target=loop, name="quebec-supervisor-heartbeat", daemon=True
        )
        self._heartbeat_thread.start()

    def _start_maintenance(self) -> None:
        def loop():
            while not self._maintenance_stop.wait(self.maintenance_interval):
                try:
                    pruned, orphaned = self.qc.supervisor_run_maintenance(
                        self._process_id
                    )
                    if pruned or orphaned:
                        logger.info(
                            "Supervisor maintenance: pruned %d process(es), "
                            "failed %d orphaned claim(s)",
                            pruned,
                            orphaned,
                        )
                except Exception as e:
                    logger.warning("Supervisor maintenance failed: %s", e)

        self._maintenance_thread = threading.Thread(
            target=loop, name="quebec-supervisor-maintenance", daemon=True
        )
        self._maintenance_thread.start()

    def _fork_child(self, role: str, index: int) -> None:
        slot = self._slots.setdefault((role, index), _SlotState())
        if slot.disabled:
            logger.warning("Slot (%s, %d) disabled; skipping restart", role, index)
            return

        now = time.monotonic()
        slot.spawn_times = [
            t for t in slot.spawn_times if now - t <= self.crash_loop_window
        ]
        slot.spawn_times.append(now)

        pid = os.fork()
        if pid == 0:
            # --- child ---
            try:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                signal.signal(signal.SIGINT, signal.SIG_DFL)
                signal.signal(signal.SIGQUIT, signal.SIG_DFL)
                os.close(self._wakeup_r)
                os.close(self._wakeup_w)

                self.qc.reset_after_fork()
                # Record ppid so role loops self-terminate if supervisor dies.
                self.qc.watch_parent_pid()

                if role == ROLE_WORKER:
                    try:
                        self.qc.apply_worker_config(index)
                    except IndexError:
                        pass  # No config file / single worker case
                    except Exception:
                        logger.exception(
                            "apply_worker_config(%d) failed in child", index
                        )
                    self.qc.run(spawn=["worker"], create_tables=False)
                elif role == ROLE_DISPATCHER:
                    try:
                        self.qc.apply_dispatcher_config(index)
                    except IndexError:
                        pass
                    except Exception:
                        logger.exception(
                            "apply_dispatcher_config(%d) failed in child", index
                        )
                    self.qc.run(spawn=["dispatcher"], create_tables=False)
                elif role == ROLE_SCHEDULER:
                    self.qc.run(spawn=["scheduler"], create_tables=False)
                else:
                    raise ValueError(f"unknown role {role!r}")
                os._exit(0)
            except SystemExit as e:
                code = e.code if isinstance(e.code, int) else 0
                os._exit(code)
            except BaseException:
                logger.exception("Child (%s, %d) crashed", role, index)
                os._exit(1)
        else:
            # --- parent ---
            logger.info("Forked %s[%d] as pid=%d", role, index, pid)
            self._children[pid] = _ChildInfo(role=role, index=index, pid=pid)

    def _supervise(self) -> None:
        while not self._stopping:
            pid = self._reap_one(block=False)
            if pid == 0:
                self._interruptible_sleep(1.0)
                continue
            self._handle_exit(pid)

    def _reap_one(self, *, block: bool) -> int:
        """Return a reaped child pid, or 0 if none (when non-blocking)."""
        flags = 0 if block else os.WNOHANG
        try:
            pid, _status = os.waitpid(-1, flags)
            return pid
        except ChildProcessError:
            return 0

    def _fail_claimed_for_pid(self, pid: int, info: Optional[_ChildInfo]) -> None:
        """Best-effort: mark any claimed jobs owned by a dead child as failed.

        Safe to call on cleanly exited children — a worker that completed its
        own `on_stop` will have already released its claims and deleted its
        process row, so this becomes a no-op. Essential on the SIGKILL path
        where the child never got to run its cleanup.
        """
        try:
            failed = self.qc.supervisor_fail_claimed_by_pid(pid, self._hostname)
            if failed:
                if info is not None:
                    logger.info(
                        "Marked %d claimed job(s) failed for %s[%d] pid=%d",
                        failed,
                        info.role,
                        info.index,
                        pid,
                    )
                else:
                    logger.info(
                        "Marked %d claimed job(s) failed for pid=%d", failed, pid
                    )
        except Exception:
            if info is not None:
                logger.exception(
                    "Failed to mark claimed jobs for %s[%d] pid=%d",
                    info.role,
                    info.index,
                    pid,
                )
            else:
                logger.exception("Failed to mark claimed jobs for pid=%d", pid)

    def _handle_exit(self, pid: int) -> None:
        info = self._children.pop(pid, None)
        if info is None:
            logger.warning("Reaped unknown pid=%d", pid)
            return
        logger.warning(
            "Child %s[%d] (pid=%d) exited unexpectedly", info.role, info.index, pid
        )

        self._fail_claimed_for_pid(pid, info)

        if self._stopping:
            return

        slot = self._slots.get((info.role, info.index))
        if slot is not None:
            now = time.monotonic()
            recent = [t for t in slot.spawn_times if now - t <= self.crash_loop_window]
            if len(recent) >= self.crash_loop_max:
                slot.disabled = True
                logger.error(
                    "Slot (%s, %d) crashed %d times in %.0fs; disabling auto-restart",
                    info.role,
                    info.index,
                    len(recent),
                    self.crash_loop_window,
                )
                return

        self._fork_child(info.role, info.index)

    def _interruptible_sleep(self, seconds: float) -> None:
        try:
            r, _, _ = select.select([self._wakeup_r], [], [], seconds)
            if r:
                try:
                    while True:
                        os.read(self._wakeup_r, 4096)
                except BlockingIOError:
                    pass
        except InterruptedError:
            pass

    def _terminate_gracefully(self) -> None:
        pids = list(self._children.keys())
        if not pids:
            return

        logger.info("Supervisor sending SIGTERM to %d child(ren)", len(pids))
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                self._children.pop(pid, None)

        deadline = time.monotonic() + self.shutdown_timeout
        while self._children and time.monotonic() < deadline:
            pid = self._reap_one(block=False)
            if pid == 0:
                time.sleep(0.1)
                continue
            info = self._children.pop(pid, None)
            self._fail_claimed_for_pid(pid, info)

        remaining = list(self._children.keys())
        for pid in remaining:
            logger.warning("Shutdown timeout; SIGKILLing pid=%d", pid)
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        while self._children:
            pid = self._reap_one(block=True)
            if pid == 0:
                break
            info = self._children.pop(pid, None)
            self._fail_claimed_for_pid(pid, info)

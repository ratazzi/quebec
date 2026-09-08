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
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union

from .cgroup import (
    EMPTY_LIMITS,
    CgroupError,
    CgroupStats,
    DisabledCgroup,
    Limits,
    config_has_explicit_limits,
    limits_from_config,
    probe,
)

logger = logging.getLogger(__name__)

ROLE_WORKER = "worker"
ROLE_DISPATCHER = "dispatcher"
ROLE_SCHEDULER = "scheduler"
VALID_ROLES = {ROLE_WORKER, ROLE_DISPATCHER, ROLE_SCHEDULER}
RECYCLE_EXIT_CODE = 75

Plan = Dict[str, Union[int, List]]


@dataclass
class _ChildInfo:
    role: str
    index: int
    pid: int


@dataclass(frozen=True)
class _ExitStatus:
    exited: bool
    exit_code: Optional[int]
    signaled: bool
    signal: Optional[int]


@dataclass
class _SlotState:
    """Restart history for a (role, index) slot."""

    spawn_times: List[float] = field(default_factory=list)
    crash_times: List[float] = field(default_factory=list)
    disabled: bool = False


def _decode_status(status: int) -> _ExitStatus:
    if os.WIFEXITED(status):
        return _ExitStatus(
            exited=True,
            exit_code=os.WEXITSTATUS(status),
            signaled=False,
            signal=None,
        )
    if os.WIFSIGNALED(status):
        return _ExitStatus(
            exited=False,
            exit_code=None,
            signaled=True,
            signal=os.WTERMSIG(status),
        )
    return _ExitStatus(exited=False, exit_code=None, signaled=False, signal=None)


def _status_description(status: _ExitStatus) -> str:
    if status.exited:
        return f"exit_code={status.exit_code}"
    if status.signaled:
        return f"signal={status.signal}"
    return "status=unknown"


def classify_exit(
    status: _ExitStatus,
    stats: Optional[CgroupStats],
    we_sent_sigkill: bool,
) -> str:
    """Label a reaped child's exit.

    The cgroup's own ``oom_kill`` counter outranks any guess made from the
    signal: SIGKILL alone cannot distinguish a kernel OOM from our own
    shutdown escalation or an operator's ``kill -9``.
    """
    if status.exited and status.exit_code == RECYCLE_EXIT_CODE:
        return "planned_recycle"
    if stats is not None and stats.oom_kill > 0:
        return "oom"
    if status.signaled and status.signal == signal.SIGKILL and we_sent_sigkill:
        return "killed_by_supervisor"
    if status.signaled:
        return "signaled"
    return "crashed"


def exceeded_own_limit(stats: Optional[CgroupStats]) -> bool:
    """Whether the kill is attributable to *this* cgroup's own memory.max.

    ``memory.events`` counts every process in the cgroup killed by any OOM
    killer, the global one included, so ``oom_kill > 0`` on its own does not
    mean the worker outgrew its own limit. A `max` event, or a peak that
    reached the limit, is what actually implicates it.
    """
    if stats is None or not stats.memory_max or stats.memory_max == "max":
        return False
    try:
        limit = int(stats.memory_max)
    except ValueError:
        return False
    if stats.max_events > 0:
        return True
    return stats.memory_peak is not None and stats.memory_peak >= limit


def oom_failure_reason(pid: int, stats: Optional[CgroupStats]) -> str:
    """Error text recorded on jobs whose worker was killed by an OOM killer.

    Written into ``failed_executions.error`` so the control plane shows the
    memory cause directly instead of a generic crash. Deliberately reports
    only what the counters prove; see :func:`exceeded_own_limit`.
    """
    parts = [f"pid={pid}"]
    if stats is not None:
        parts.append(f"oom_kill={stats.oom_kill}")
        if stats.memory_max:
            parts.append(f"memory.max={stats.memory_max}")
        if stats.memory_peak is not None:
            parts.append(f"memory.peak={stats.memory_peak}")
    reason = "Worker process killed by the OOM killer (" + ", ".join(parts) + ")."
    if exceeded_own_limit(stats):
        reason += " Likely exceeded this worker's memory.max."
    return reason


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
        cgroup: Cgroup backend override. Defaults to probing the host; pass a
            ``DisabledCgroup`` or a ``CgroupManager`` rooted at a temp dir in
            tests.
        limits: Per-role cgroup limits keyed like
            ``Quebec.supervisor_resource_limits_from_config()``. Defaults to
            reading queue.yml through ``qc``.
    """

    def __init__(
        self,
        qc,
        plan: Plan,
        *,
        control_plane: Optional[str] = None,
        shutdown_timeout: Optional[float] = None,
        crash_loop_window: float = 10.0,
        crash_loop_max: int = 3,
        heartbeat_interval: float = 60.0,
        maintenance_interval: float = 300.0,
        cgroup=None,
        limits: Optional[Dict] = None,
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
        # Match Solid Queue's default (`SolidQueue.shutdown_timeout = 5`).
        self.shutdown_timeout = (
            shutdown_timeout if shutdown_timeout is not None else 5.0
        )
        self.crash_loop_window = crash_loop_window
        self.crash_loop_max = crash_loop_max
        self.heartbeat_interval = heartbeat_interval
        self.maintenance_interval = maintenance_interval

        self._process_id: Optional[int] = None
        self._children: Dict[int, _ChildInfo] = {}
        self._slots: Dict[Tuple[str, int], _SlotState] = {}
        self._stopping = False
        self._immediate = False
        self._quiet = False
        self._hostname = socket.gethostname()
        self._wakeup_r, self._wakeup_w = os.pipe()
        os.set_blocking(self._wakeup_r, False)
        os.set_blocking(self._wakeup_w, False)
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop = threading.Event()
        self._maintenance_thread: Optional[threading.Thread] = None
        self._maintenance_stop = threading.Event()
        # pids we SIGKILLed ourselves, so an exit signal of SIGKILL is not
        # mistaken for something the kernel did.
        self._sigkilled: set = set()
        self._cgroup_ready = False
        # True only while start() is standing up the initial fleet. A cgroup
        # failure then is fatal (see §3.6); the same failure on a later refork
        # must not take down the slots that are running fine.
        self._starting = True
        # Slots whose refork failed and should be retried by the supervise loop.
        self._pending_forks: set = set()

        self._cgroup = probe() if cgroup is None else cgroup
        raw_limits = self._load_config_limits() if limits is None else limits
        self._configured_limits = config_has_explicit_limits(raw_limits)
        if not self._cgroup.enabled and self._configured_limits:
            raise RuntimeError(
                "queue.yml configures cgroup memory limits, but no usable cgroup v2 "
                f"subtree is available: {getattr(self._cgroup, 'reason', 'unknown')}. "
                "Run under systemd with Delegate=yes, run as root, point "
                "QUEBEC_CGROUP_ROOT at a delegated subtree, or remove the limits."
            )
        if not self._cgroup.enabled:
            logger.warning(
                "cgroup limits are unavailable (%s); supervisor continues without them",
                getattr(self._cgroup, "reason", "unknown"),
            )
        self._slot_limits = self._resolve_slot_limits(raw_limits)

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
            self._setup_cgroup_root()
            for role, count in self.plan.items():
                for index in range(count):
                    # A SIGTERM/SIGQUIT arriving mid-startup sets _stopping via
                    # the signal handler. Stop forking the rest of the plan
                    # instead of standing up the whole fleet only to tear it
                    # down immediately in _supervise().
                    if self._stopping:
                        break
                    self._fork_child(role, index)
                if self._stopping:
                    break

            # Treat each startup phase as a separate checkpoint. Signal handlers
            # run on this main thread, including after a blocking startup call
            # returns, so re-check before every later phase instead of relying on
            # one guard around the whole block.
            #
            # Start the control plane only after all children are forked so the
            # listening socket/runtime task stays parent-only. Children would
            # otherwise inherit the TcpListener fd.
            if not self._stopping and self.control_plane:
                try:
                    self.qc.start_control_plane(self.control_plane)
                except Exception:
                    logger.exception("Failed to start control plane")

            # The initial fleet is up: from here a cgroup failure is a
            # single-slot problem, not a reason to tear everything down.
            self._starting = False

            if not self._stopping:
                self._start_heartbeat()

            if not self._stopping:
                self._start_maintenance()

            if not self._stopping:
                # Children forked and control plane up: tell systemd we're
                # ready. No-op unless launched under a Type=notify unit.
                self.qc.systemd_ready(self._status_text())

            self._supervise()
        finally:
            try:
                self.qc.systemd_stop()
                self._terminate_gracefully()
            finally:
                self._heartbeat_stop.set()
                self._maintenance_stop.set()
                if self._heartbeat_thread is not None:
                    self._heartbeat_thread.join(timeout=2.0)
                if self._maintenance_thread is not None:
                    self._maintenance_thread.join(timeout=2.0)
                if self._cgroup_ready:
                    self._cgroup.scavenge()
                self._cgroup.close()
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

    def _setup_cgroup_root(self) -> None:
        """Vacate the delegated root, enable controllers, sweep leftovers.

        Ordering is mandated by the v2 "no internal process" rule: the
        supervisor has to move into its own leaf before controllers can be
        enabled for the children.
        """
        if not self._cgroup.enabled:
            return
        try:
            self._cgroup.prepare()
        except (OSError, CgroupError) as exc:
            if self._configured_limits or any(
                limits.must_enforce() for limits in self._slot_limits.values()
            ):
                raise RuntimeError(
                    f"cannot prepare cgroup root {getattr(self._cgroup, 'root', '?')}: "
                    f"{exc}. Configured memory limits cannot be enforced."
                ) from exc
            logger.warning(
                "Cannot prepare cgroup root %s: %s; continuing without cgroups",
                getattr(self._cgroup, "root", "?"),
                exc,
            )
            self._cgroup = DisabledCgroup(f"prepare failed: {exc}")
            return
        self._cgroup_ready = True
        self._cgroup.scavenge()

    def _load_config_limits(self) -> Dict:
        try:
            return self.qc.supervisor_resource_limits_from_config() or {}
        except Exception:
            # No queue.yml / older core: fall through to no limits. A missing
            # config cannot be a hard error here, the low-level Supervisor API
            # is documented to work without one.
            logger.debug("No cgroup limits available from config", exc_info=True)
            return {}

    def _resolve_slot_limits(
        self, raw: Optional[Dict]
    ) -> Dict[Tuple[str, int], Limits]:
        """Expand config limits per slot, deriving memory.max where needed.

        Derivation only runs when a cgroup is actually usable, so an existing
        deployment that only sets ``memory_recycle_at`` keeps booting on hosts
        without cgroups.
        """
        raw = raw or {}
        default_rss = raw.get("default_worker_max_rss_bytes")
        resolved: Dict[Tuple[str, int], Limits] = {}
        for role, count in self.plan.items():
            entries = raw.get(role) or []
            for index in range(count):
                entry = dict(entries[index]) if index < len(entries) else {}
                if role == ROLE_WORKER and "worker_max_rss_bytes" not in entry:
                    entry["worker_max_rss_bytes"] = default_rss
                limits = limits_from_config(entry, derive=self._cgroup.enabled)
                if limits.derived:
                    logger.info(
                        "%s[%d]: cgroup memory.max derived from memory_recycle_at=%dMiB "
                        "-> %dMiB (x%.1f; set memory_max explicitly to override)",
                        role,
                        index,
                        (entry.get("worker_max_rss_bytes") or 0) // (1024 * 1024),
                        (limits.memory_max or 0) // (1024 * 1024),
                        1.5,
                    )
                resolved[(role, index)] = limits
        return resolved

    def _install_signal_handlers(self) -> None:
        def graceful(signum, _frame):
            logger.info("Supervisor received signal %d", signum)
            self._stopping = True
            self._wake()

        # SIGQUIT skips the graceful-wait window and SIGKILLs children at once.
        # Mirrors Solid Queue's terminate_immediately path for the QUIT signal.
        def immediate(signum, _frame):
            logger.warning(
                "Supervisor received signal %d, terminating children immediately",
                signum,
            )
            self._stopping = True
            self._immediate = True
            self._wake()

        # Sidekiq-style quiet: supervisor doesn't claim jobs itself, so we
        # cascade the signal to every worker child. Dispatcher/scheduler are
        # unaffected (they don't own in-flight job execution).
        def quiet(signum, _frame):
            self._quiet = True
            logger.info(
                "Supervisor received signal %d, forwarding quiet (SIGUSR1) "
                "to worker children",
                signum,
            )
            for pid, info in list(self._children.items()):
                if info.role != ROLE_WORKER:
                    continue
                try:
                    os.kill(pid, signal.SIGUSR1)
                except ProcessLookupError:
                    pass
                except OSError as e:
                    logger.warning(
                        "Failed to forward quiet to worker pid=%d: %s", pid, e
                    )

        signal.signal(signal.SIGTERM, graceful)
        signal.signal(signal.SIGINT, graceful)
        signal.signal(signal.SIGQUIT, immediate)
        # SIGUSR1 is the always-on quiet trigger.
        signal.signal(signal.SIGUSR1, quiet)
        # In an interactive terminal Ctrl-Z must keep its shell-job-control
        # meaning, so only intercept SIGTSTP when stdin isn't a tty (daemon /
        # systemd / docker / nohup).
        try:
            stdin_is_tty = sys.stdin.isatty()
        except (AttributeError, ValueError):
            stdin_is_tty = False
        if not stdin_is_tty and hasattr(signal, "SIGTSTP"):
            signal.signal(signal.SIGTSTP, quiet)

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

    def _run_maintenance_once(self) -> None:
        try:
            pruned, orphaned = self.qc.supervisor_run_maintenance(self._process_id)
            if pruned or orphaned:
                logger.info(
                    "Supervisor maintenance: pruned %d process(es), "
                    "failed %d orphaned claim(s)",
                    pruned,
                    orphaned,
                )
        except Exception as e:
            logger.warning("Supervisor maintenance failed: %s", e)

    def _start_maintenance(self) -> None:
        # Solid Queue runs `fail_orphaned_executions` once at boot and starts
        # the prune timer with `run_now: true`. Mirror that by sweeping
        # immediately before the periodic loop so a fresh supervisor cleans up
        # stale state from the previous instance instead of waiting one full
        # interval.
        self._run_maintenance_once()

        def loop():
            while not self._maintenance_stop.wait(self.maintenance_interval):
                self._run_maintenance_once()

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

        limits = self._slot_limits.get((role, index), EMPTY_LIMITS)
        try:
            placed_in_cgroup = self._create_slot_cgroup(role, index, limits)
        except CgroupError as exc:
            if self._starting:
                raise
            # Runtime refork: fail this slot only. Retried by _supervise until
            # the crash-loop guard disables it.
            logger.error(
                "Cannot prepare cgroup for %s[%d]: %s; leaving the slot down "
                "for now (its configured memory limit would not be enforced)",
                role,
                index,
                exc,
            )
            if not self._record_slot_crash(role, index):
                self._pending_forks.add((role, index))
            else:
                self._pending_forks.discard((role, index))
            return

        # Barrier so the child does not run before the parent has migrated it
        # into its cgroup. Without it, everything the child allocates between
        # fork() and the migration is charged to the supervisor's cgroup and
        # stays there (v2 does not move existing charges).
        barrier_r, barrier_w = os.pipe()

        pid = os.fork()
        if pid == 0:
            # --- child ---
            try:
                self._cgroup.close()
                os.close(barrier_w)
                try:
                    go = os.read(barrier_r, 1)
                except OSError:
                    go = b""
                os.close(barrier_r)
                if not go:
                    # Parent closed the pipe without releasing us: either it
                    # died, or placing us in the cgroup failed and it wants
                    # this child gone.
                    os._exit(1)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                signal.signal(signal.SIGINT, signal.SIG_DFL)
                signal.signal(signal.SIGQUIT, signal.SIG_DFL)
                os.close(self._wakeup_r)
                os.close(self._wakeup_w)
                # Only the supervisor (main PID) talks to systemd; drop the
                # socket so a child can never feed the watchdog or send status.
                os.environ.pop("NOTIFY_SOCKET", None)
                # This child runs a single role, not another supervisor. Drop
                # QUEBEC_SUPERVISOR so the `qc.run(spawn=[...])` below takes the
                # single-process path instead of re-entering supervisor mode
                # (which would recurse: supervisor_plan_from_config sees the
                # same queue.yml, forks again, ad infinitum).
                os.environ.pop("QUEBEC_SUPERVISOR", None)

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
            os.close(barrier_r)
            # Track the child before placement: a startup exception must leave
            # it visible to start()'s shutdown/reap path.
            self._children[pid] = _ChildInfo(role=role, index=index, pid=pid)
            try:
                release = True
                if placed_in_cgroup:
                    release = self._place_in_cgroup(pid, role, index, limits)
                if release:
                    try:
                        os.write(barrier_w, b"x")
                    except OSError:
                        pass
            finally:
                # EOF releases an aborted child without running user code.
                os.close(barrier_w)
            logger.info("Forked %s[%d] as pid=%d", role, index, pid)

    def _create_slot_cgroup(self, role: str, index: int, limits: Limits) -> bool:
        """Create this slot's leaf cgroup before forking. Returns usability."""
        if not self._cgroup_ready:
            return False
        try:
            self._cgroup.create(role, index, limits)
        except CgroupError as exc:
            if not limits.must_enforce():
                logger.warning(
                    "Cannot create cgroup for %s[%d]: %s; starting it unconstrained",
                    role,
                    index,
                    exc,
                )
                return False
            # A configured limit that cannot be applied must not be silently
            # dropped, so this slot's fork is treated as a failure.
            logger.error("Cannot create cgroup for %s[%d]: %s", role, index, exc)
            raise
        return True

    def _place_in_cgroup(self, pid: int, role: str, index: int, limits: Limits) -> bool:
        """Migrate the child. Returns whether it should be released to run."""
        try:
            # False means ESRCH: the child died before we could move it, which
            # the normal reap path already handles.
            self._cgroup.place(pid, role, index)
            return True
        except CgroupError as exc:
            if not limits.must_enforce():
                logger.warning(
                    "Cannot place %s[%d] (pid=%d) in its cgroup: %s; "
                    "running it unconstrained",
                    role,
                    index,
                    pid,
                    exc,
                )
                return True
            logger.error(
                "Cannot place %s[%d] (pid=%d) in its cgroup: %s; aborting this child "
                "because its configured memory limit would not be enforced",
                role,
                index,
                pid,
                exc,
            )
            if self._starting:
                raise
            return False

    def _status_text(self) -> str:
        """Build the systemd STATUS line from in-process state.

        Uses the live child set (`self._children`) and the configured plan —
        not a database query — so it reflects exactly the processes this
        supervisor manages, never a global, cross-node view.
        """
        from .quebec import __version__

        counts: Dict[str, int] = {}
        for info in self._children.values():
            counts[info.role] = counts.get(info.role, 0) + 1
        segs = []
        for role in (ROLE_WORKER, ROLE_DISPATCHER, ROLE_SCHEDULER):
            expected = self.plan.get(role, 0)
            if expected:
                segs.append(f"{role}s {counts.get(role, 0)}/{expected}")
        body = ", ".join(segs) if segs else "starting"
        if self._stopping:
            state = "stopping"
        elif self._quiet:
            state = "quiet"
        else:
            state = "running"
        return f"Quebec {__version__}: supervisor; {body}; {state}"

    def _retry_pending_forks(self) -> None:
        """Re-attempt reforks whose cgroup setup failed a moment ago."""
        for key in sorted(self._pending_forks):
            slot = self._slots.get(key)
            if slot is not None and slot.disabled:
                self._pending_forks.discard(key)
                continue
            self._pending_forks.discard(key)
            self._fork_child(*key)

    def _supervise(self) -> None:
        while not self._stopping:
            # Refresh status + pet the systemd watchdog. Driving this from the
            # supervise loop means a hung loop stops petting and lets systemd
            # restart us. No-op unless launched under a Type=notify unit.
            self.qc.systemd_notify(self._status_text())
            if self._pending_forks:
                self._retry_pending_forks()
            pid, status = self._reap_one(block=False)
            if pid == 0:
                self._interruptible_sleep(1.0)
                continue
            self._handle_exit(pid, status)

    def _reap_one(self, *, block: bool) -> Tuple[int, Optional[_ExitStatus]]:
        """Return a reaped child pid/status, or ``(0, None)`` if none."""
        flags = 0 if block else os.WNOHANG
        try:
            pid, status = os.waitpid(-1, flags)
            if pid == 0:
                return 0, None
            return pid, _decode_status(status)
        except ChildProcessError:
            return 0, None

    def _fail_claimed_for_pid(
        self, pid: int, info: Optional[_ChildInfo], reason: Optional[str] = None
    ) -> None:
        """Best-effort: mark any claimed jobs owned by a dead child as failed.

        Safe to call on cleanly exited children — a worker that completed its
        own `on_stop` will have already released its claims and deleted its
        process row, so this becomes a no-op. Essential on the SIGKILL path
        where the child never got to run its cleanup.
        """
        try:
            # Keep the two-argument call on the unattributed path so behaviour
            # is unchanged wherever cgroups are not in play.
            failed = (
                self.qc.supervisor_fail_claimed_by_pid(pid, self._hostname)
                if reason is None
                else self.qc.supervisor_fail_claimed_by_pid(pid, self._hostname, reason)
            )
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

    def _handle_exit(self, pid: int, status: Optional[_ExitStatus]) -> None:
        status = status or _ExitStatus(
            exited=False, exit_code=None, signaled=False, signal=None
        )
        info = self._children.pop(pid, None)
        if info is None:
            logger.warning("Reaped unknown pid=%d", pid)
            return

        kind, cg_stats = self._reclaim_child(pid, info, status)
        oom = kind == "oom"

        if kind == "planned_recycle" and info.role == ROLE_WORKER:
            if self._stopping:
                return
            logger.info(
                "Child %s[%d] (pid=%d) exited for planned memory recycle; reforking",
                info.role,
                info.index,
                pid,
            )
            self._fork_child(info.role, info.index)
            return

        if self._stopping:
            return

        if oom:
            logger.error(
                "Child %s[%d] (pid=%d) was killed by the OOM killer (%s)",
                info.role,
                info.index,
                pid,
                self._describe_cgroup_stats(cg_stats),
            )
        else:
            logger.warning(
                "Child %s[%d] (pid=%d) exited unexpectedly (%s)",
                info.role,
                info.index,
                pid,
                _status_description(status),
            )

        # OOM shares this counter on purpose: a memory_max too small to even
        # boot the interpreter would otherwise fork-loop forever.
        hint = (
            f" Last exit was an OOM kill ({self._describe_cgroup_stats(cg_stats)});"
            " raise memory_max for this entry, or lower threads."
            if oom
            else ""
        )
        if self._record_slot_crash(info.role, info.index, hint):
            return

        self._fork_child(info.role, info.index)

    def _reclaim_child(
        self,
        pid: int,
        info: Optional[_ChildInfo],
        status: Optional[_ExitStatus],
    ) -> Tuple[str, Optional[CgroupStats]]:
        """The tail every reap shares: snapshot, classify, attribute, destroy.

        Every path that reaps a child goes through here, shutdown included:
        a worker OOM-killed while draining deserves the same attribution as
        one killed mid-run, and the cgroup snapshot has to be taken before
        the directory is removed either way.
        """
        status = status or _ExitStatus(
            exited=False, exit_code=None, signaled=False, signal=None
        )
        cg_stats = self._cgroup.stats(info.role, info.index) if info else None
        we_sent_sigkill = pid in self._sigkilled
        self._sigkilled.discard(pid)
        kind = classify_exit(status, cg_stats, we_sent_sigkill)

        planned_recycle = (
            info is not None and info.role == ROLE_WORKER and kind == "planned_recycle"
        )
        if not planned_recycle:
            # Two-argument call on the unattributed path keeps the pre-cgroup
            # behaviour (and call shape) untouched.
            if kind == "oom":
                self._fail_claimed_for_pid(pid, info, oom_failure_reason(pid, cg_stats))
            else:
                self._fail_claimed_for_pid(pid, info)
        if info is not None:
            self._cgroup.destroy(info.role, info.index)
        return kind, cg_stats

    def _record_slot_crash(self, role: str, index: int, hint: str = "") -> bool:
        """Count one failure against a slot. Returns True once it is disabled."""
        slot = self._slots.get((role, index))
        if slot is None:
            return False
        now = time.monotonic()
        slot.crash_times = [
            t for t in slot.crash_times if now - t <= self.crash_loop_window
        ]
        slot.crash_times.append(now)
        if len(slot.crash_times) < self.crash_loop_max:
            return False
        slot.disabled = True
        logger.error(
            "Slot (%s, %d) crashed %d times in %.0fs; disabling auto-restart.%s",
            role,
            index,
            len(slot.crash_times),
            self.crash_loop_window,
            hint,
        )
        return True

    @staticmethod
    def _describe_cgroup_stats(stats: Optional[CgroupStats]) -> str:
        if stats is None:
            return "no cgroup stats"
        parts = []
        if stats.memory_max:
            parts.append(f"memory.max={stats.memory_max}")
        if stats.memory_peak is not None:
            parts.append(f"memory.peak={stats.memory_peak}")
        parts.append(f"oom_kill={stats.oom_kill}")
        return ", ".join(parts)

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

        # Solid Queue mirror:
        #   * SIGTERM phase (graceful) -> wait shutdown_timeout
        #   * SIGQUIT phase (terminate_immediately) -> children call exit(0)
        #     via the supervised SIGQUIT handler in src/types.rs
        #   * SIGKILL is a final safety net for processes wedged in C code that
        #     never reach the SIGQUIT handler (Solid Queue stops at SIGQUIT and
        #     leaves wedged children to init).
        if self._immediate:
            self._kill_children(pids, signal.SIGQUIT, "SIGQUIT")
        else:
            self._kill_children(pids, signal.SIGTERM, "SIGTERM")
            self._reap_until(deadline=time.monotonic() + self.shutdown_timeout)
            if self._children:
                self._kill_children(
                    list(self._children.keys()), signal.SIGQUIT, "SIGQUIT"
                )

        # Short window for SIGQUIT handlers to run before final SIGKILL.
        self._reap_until(deadline=time.monotonic() + 1.0)

        remaining = list(self._children.keys())
        for pid in remaining:
            logger.warning("Shutdown timeout; SIGKILLing pid=%d", pid)
            self._sigkilled.add(pid)
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        while self._children:
            pid, status = self._reap_one(block=True)
            if pid == 0:
                break
            self._reclaim_child(pid, self._children.pop(pid, None), status)

    def _kill_children(self, pids: List[int], sig: int, sig_name: str) -> None:
        logger.info("Supervisor sending %s to %d child(ren)", sig_name, len(pids))
        for pid in pids:
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                self._children.pop(pid, None)

    def _reap_until(self, *, deadline: float) -> None:
        while self._children and time.monotonic() < deadline:
            pid, status = self._reap_one(block=False)
            if pid == 0:
                time.sleep(0.1)
                continue
            self._reclaim_child(pid, self._children.pop(pid, None), status)

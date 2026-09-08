"""cgroup v2 leaf cgroups for the fork-based supervisor.

Each supervised child gets its own leaf cgroup under a delegated subtree, so a
runaway job is killed by the kernel as a unit (``memory.oom.group``) instead of
taking the whole host with it. The supervisor reads ``memory.events`` after
reaping a child to tell a cgroup OOM apart from an ordinary crash.

Everything here is Linux-only and best-effort by default: when no subtree is
writable and no limits are configured, :func:`probe` returns a
:class:`DisabledCgroup` whose methods are no-ops, and the supervisor behaves
exactly as it did before. When limits *are* configured, an unusable subtree is
a startup error instead — silently dropping a configured ``memory_max`` would
leave a deployment believing it has a protection it does not have.

Only cgroup v2 (unified hierarchy) is supported. v1 lacks ``memory.oom.group``
and ``cgroup.kill``, which the OOM attribution and cleanup paths rely on.
"""

from __future__ import annotations

import errno
import logging
import os
import re
import signal
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple, Union

logger = logging.getLogger(__name__)

ENABLE_ENV = "QUEBEC_CGROUP"
ROOT_ENV = "QUEBEC_CGROUP_ROOT"

SUPERVISOR_LEAF = "supervisor"
#: Only directories matching this are ever touched by scavenge/destroy, so a
#: shared cgroup root cannot have unrelated directories removed.
SLOT_DIR_RE = re.compile(r"^(worker|dispatcher|scheduler)-\d+(\.\d+)?$")

#: `memory.max` is derived from `memory_recycle_at` times this factor. RSS and
#: `memory.current` are different quantities (the latter includes page cache),
#: so the hard limit has to sit well above the soft recycle line.
DERIVE_FACTOR = 1.5

REQUIRED_CONTROLLER = "memory"

#: What queue.yml's `max` becomes once parsed. Kept distinct from `None` (not
#: configured at all) so an explicit "no limit" neither derives a limit nor
#: pulls in the swap/oom defaults that a real limit implies.
UNLIMITED = "max"


def is_limit_value(value) -> bool:
    """True for a numeric limit, false for absent or an explicit `max`."""
    return isinstance(value, int) and not isinstance(value, bool)


class CgroupError(Exception):
    """A cgroup operation failed in a way the caller must act on."""


@dataclass(frozen=True)
class Limits:
    """Resolved cgroup memory limits for one supervisor slot."""

    # int = a byte count, UNLIMITED = an explicit `max`, None = unset.
    memory_max: Optional[Union[int, str]] = None
    memory_high: Optional[Union[int, str]] = None
    memory_swap_max: Optional[Union[int, str]] = None
    memory_oom_group: Optional[bool] = None
    #: True when `memory_max` was derived from `memory_recycle_at`.
    derived: bool = False

    def must_enforce(self) -> bool:
        """Whether these limits actually change kernel behaviour.

        The single judgement behind every "is this failure fatal?" decision.
        An explicit `max` asks the kernel for nothing, so failing to apply it
        is not worth aborting over; a numeric limit, or an explicit
        `memory_oom_group`, does change what happens and must be enforced.
        """
        return (
            is_limit_value(self.memory_max)
            or is_limit_value(self.memory_high)
            or is_limit_value(self.memory_swap_max)
            or self.memory_oom_group is not None
        )


EMPTY_LIMITS = Limits()


@dataclass(frozen=True)
class CgroupStats:
    """Snapshot of a child's cgroup taken right after it was reaped."""

    oom_kill: int = 0
    oom_group_kill: int = 0
    max_events: int = 0
    memory_peak: Optional[int] = None
    memory_max: Optional[str] = None


def parse_size_bytes(value) -> Optional[int]:
    """Parse a memory size the way Rust's ``parse_size`` does.

    Only used for limits injected directly from Python (tests, embedded use);
    values coming from queue.yml are already parsed on the Rust side.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    text = str(value).strip()
    if not text or text.lower() == "max":
        return None
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([A-Za-z]*)", text)
    if not match:
        return None
    number = float(match.group(1))
    unit = match.group(2).lower()
    multipliers = {
        "": 1,
        "b": 1,
        "k": 1024,
        "ki": 1024,
        "kib": 1024,
        "kb": 1000,
        "m": 1024**2,
        "mi": 1024**2,
        "mib": 1024**2,
        "mb": 1000**2,
        "g": 1024**3,
        "gi": 1024**3,
        "gib": 1024**3,
        "gb": 1000**3,
        "t": 1024**4,
        "ti": 1024**4,
        "tib": 1024**4,
        "tb": 1000**4,
    }
    if unit not in multipliers:
        return None
    return round(number * multipliers[unit])


def resolve_memory_max(
    explicit: Optional[int],
    worker_max_rss_bytes: Optional[int],
    factor: float = DERIVE_FACTOR,
) -> Tuple[Optional[int], bool]:
    """Return ``(memory_max_bytes, derived)``.

    An explicit value always wins. Otherwise the soft RSS recycle line is
    scaled up to a hard limit, rounded up to whole MiB. Callers must only
    invoke this once the cgroup probe has succeeded — a derived limit that
    cannot be applied is a startup error, and a machine without cgroups must
    keep booting.
    """
    # An explicit `max` is a decision too: never derive over the top of it.
    if explicit is not None:
        return explicit, False
    if not worker_max_rss_bytes:
        return None, False
    mib = 1024 * 1024
    # Integer ceil-div on the scaled byte count: doing the division in floats
    # makes exact multiples (100MiB x 1.1) land just above the boundary and
    # round up a whole extra MiB.
    scaled = round(worker_max_rss_bytes * factor)
    derived = -(-scaled // mib) * mib
    return derived, True


def limits_from_config(entry: Optional[Dict], *, derive: bool) -> Limits:
    """Build :class:`Limits` from one slot dict of
    ``Quebec.supervisor_resource_limits_from_config``.

    ``derive`` is the probe result: derivation only happens when a cgroup is
    actually usable.
    """
    entry = entry or {}
    explicit_max = entry.get("memory_max")
    memory_max, derived = (
        resolve_memory_max(explicit_max, entry.get("worker_max_rss_bytes"))
        if derive
        else (explicit_max, False)
    )

    swap_max = entry.get("memory_swap_max")
    oom_group = entry.get("memory_oom_group")
    # Only a real byte count implies the companion defaults. `memory_max: max`
    # says "deliberately no limit", so turning swap off and enabling
    # oom.group off the back of it would be the opposite of what was asked.
    if is_limit_value(memory_max):
        # Without a swap cap, `memory.max` only pushes the worker into swap:
        # it stops dying but becomes unusably slow. And an OOM that kills one
        # grandchild instead of the worker leaves the job stuck with a
        # misleading error, so kill the cgroup as a unit by default.
        if swap_max is None:
            swap_max = 0
        if oom_group is None:
            oom_group = True

    return Limits(
        memory_max=memory_max,
        memory_high=entry.get("memory_high"),
        memory_swap_max=swap_max,
        memory_oom_group=oom_group,
        derived=derived,
    )


def config_has_explicit_limits(raw: Optional[Dict]) -> bool:
    """True when queue.yml sets any cgroup limit for any slot.

    Drives the failure policy: configured limits that cannot be applied abort
    startup, an unusable cgroup with nothing configured only warns.
    """
    if not raw:
        return False
    for role, entries in raw.items():
        if role == "default_worker_max_rss_bytes" or not isinstance(entries, list):
            continue
        for entry in entries:
            # Same judgement the runtime paths use, so "probe failed" and
            # "prepare failed" cannot disagree about what is fatal.
            # derive=False: a derived limit is not a *configured* one.
            if limits_from_config(entry, derive=False).must_enforce():
                return True
    return False


def parse_proc_self_cgroup(text: str) -> Optional[str]:
    """Return the v2 path from ``/proc/self/cgroup`` (the ``0::<path>`` line)."""
    for line in text.splitlines():
        parts = line.split(":", 2)
        if len(parts) == 3 and parts[0] == "0" and parts[1] == "":
            return parts[2] or "/"
    return None


def parse_cgroup2_mountpoint(text: str) -> Optional[str]:
    """Return the cgroup2 mount point from ``/proc/mounts`` content."""
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 3 and fields[2] == "cgroup2":
            return fields[1]
    return None


def parse_keyed_file(text: str) -> Dict[str, int]:
    """Parse a ``key value`` cgroup file such as ``memory.events``."""
    out: Dict[str, int] = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        try:
            out[fields[0]] = int(fields[1])
        except ValueError:
            continue
    return out


def _read(path: str) -> Optional[str]:
    try:
        with open(path, "r") as fh:
            return fh.read()
    except OSError:
        return None


def _write(path: str, value: str) -> None:
    """Write one cgroup control file.

    cgroupfs wants the whole value in a single unbuffered write, so this uses
    a raw fd rather than a buffered file object. ``O_CREAT`` never fires on a
    real hierarchy (the kernel materialises every interface file on mkdir) but
    lets the unit tests drive a plain directory tree.
    """
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o644)
    try:
        os.write(fd, value.encode())
    finally:
        os.close(fd)


def _lock_directory(path: str, *, blocking: bool = True) -> int:
    # Imported only for the enabled backend; Windows can still import the
    # module and use DisabledCgroup. cgroupfs cannot contain lock files, but
    # its directories themselves support flock.
    import fcntl

    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
        fcntl.flock(fd, flags)
        return fd
    except BaseException:
        os.close(fd)
        raise


@contextmanager
def _directory_lock(path: str):
    fd = _lock_directory(path)
    try:
        yield
    finally:
        os.close(fd)


class DisabledCgroup:
    """Null object used whenever cgroups are unavailable or turned off.

    Every method is a no-op so the supervisor can call them unconditionally;
    there is deliberately no ``if cgroup is not None`` anywhere in the caller.
    """

    enabled = False

    def __init__(self, reason: str):
        self.reason = reason

    def prepare(self) -> None:
        pass

    def scavenge(self) -> None:
        pass

    def create(self, role: str, index: int, limits: Limits) -> None:
        pass

    def place(self, pid: int, role: str, index: int) -> bool:
        return True

    def stats(self, role: str, index: int) -> Optional[CgroupStats]:
        return None

    def destroy(self, role: str, index: int) -> None:
        pass

    def path_for(self, role: str, index: int) -> Optional[str]:
        return None

    def close(self) -> None:
        pass

    def __repr__(self) -> str:
        return f"DisabledCgroup(reason={self.reason!r})"


class CgroupManager:
    """Manages one leaf cgroup per supervised child under ``root``."""

    enabled = True

    def __init__(self, root: str, controllers: Iterable[str] = ("memory",)):
        self.root = root
        self.controllers = frozenset(controllers)
        # (role, index) -> the directory this slot's *current* child owns.
        # Not always `{role}-{index}`: see _claim_path.
        self._paths: Dict[Tuple[str, int], str] = {}
        self._locks: Dict[Tuple[str, int], int] = {}

    def close(self) -> None:
        """Release ownership, also used to close inherited fds after fork.

        Do not explicitly LOCK_UN: a forked child shares the parent's open
        file descriptions, and unlocking there would unlock the parent too.
        """
        for fd in self._locks.values():
            os.close(fd)
        self._locks.clear()
        self._paths.clear()

    def __del__(self):
        self.close()

    def path_for(self, role: str, index: int) -> Optional[str]:
        """The directory the slot's live child occupies, if it has one."""
        return self._paths.get((role, index))

    def _claim_path(self, role: str, index: int) -> str:
        """Create a directory this child owns outright, and return it.

        `os.mkdir` without `exist_ok` is the whole point: it is the atomic
        exclusive-create the kernel gives us. Checking "is it empty?" and then
        creating would be two steps with a gap, and two supervisors racing
        through that gap (or one supervisor claiming a directory the previous
        one has not destroyed yet) end up sharing a cgroup — where the loser's
        `destroy()` rmdirs and kills the winner's child.

        So an existing directory is *never* reused, empty or not. Leftovers
        are cleaned by :meth:`scavenge` at the next startup, never from here.
        A directory nobody has touched also guarantees the `memory.events`
        counters start at zero, which is what makes them attributable.
        """
        base = os.path.join(self.root, f"{role}-{index}")
        try:
            # Serialize mkdir + ownership lock with every scavenger. Holding
            # only the leaf lock would leave a gap between mkdir and flock.
            with _directory_lock(self.root):
                for attempt in range(1, 100):
                    candidate = base if attempt == 1 else f"{base}.{attempt}"
                    try:
                        os.mkdir(candidate)
                    except FileExistsError:
                        continue
                    try:
                        fd = _lock_directory(candidate, blocking=False)
                    except OSError:
                        os.rmdir(candidate)
                        raise
                    self._locks[(role, index)] = fd
                    if attempt > 1:
                        logger.warning(
                            "cgroup %s already exists; %s[%d] claimed %s instead",
                            base,
                            role,
                            index,
                            candidate,
                        )
                    return candidate
        except OSError as exc:
            raise CgroupError(f"cannot claim cgroup under {base}: {exc}") from exc
        raise CgroupError(f"no free cgroup directory for {role}[{index}] under {base}")

    def prepare(self) -> None:
        """Move the supervisor into its own leaf, then enable controllers.

        The order matters: cgroup v2 refuses to write ``cgroup.subtree_control``
        while the cgroup still has member processes ("no internal process"
        constraint), so the supervisor has to vacate the root first.
        """
        root_procs = os.path.join(self.root, "cgroup.procs")
        pids = {
            line.strip() for line in (_read(root_procs) or "").split() if line.strip()
        }
        if str(os.getpid()) in pids:
            leaf = os.path.join(self.root, SUPERVISOR_LEAF)
            os.makedirs(leaf, exist_ok=True)
            # Writing to cgroup.procs migrates the whole thread group, so the
            # heartbeat/maintenance threads follow us regardless of ordering.
            _write(os.path.join(leaf, "cgroup.procs"), str(os.getpid()))

        enabled = set(
            (_read(os.path.join(self.root, "cgroup.subtree_control")) or "").split()
        )
        missing = sorted(c for c in self.controllers if c not in enabled)
        if not missing:
            return

        # Vacating our own pid is not enough if something else shares the root:
        # the kernel answers EBUSY, which on its own tells the operator nothing.
        # Our own pid is excluded: we just migrated it out, and a real
        # hierarchy has already dropped it from this list.
        leftover = {
            p.strip()
            for p in (_read(root_procs) or "").split()
            if p.strip() and p.strip() != str(os.getpid())
        }
        if leftover:
            raise CgroupError(
                f"cgroup root {self.root} still holds {len(leftover)} other "
                f"process(es) (pids {','.join(sorted(leftover))}); cgroup v2 cannot "
                "enable controllers for a cgroup that has member processes. Give "
                "Quebec a cgroup of its own (systemd Delegate=yes, or point "
                "QUEBEC_CGROUP_ROOT at a dedicated empty subtree)."
            )

        _write(
            os.path.join(self.root, "cgroup.subtree_control"),
            " ".join(f"+{c}" for c in missing),
        )

    def scavenge(self) -> None:
        """Remove leftover slot cgroups from a previous supervisor.

        A non-empty leftover is left alone: during a rolling restart the old
        children may still be draining, and killing them here would fail jobs
        that were about to finish.
        """
        try:
            with _directory_lock(self.root):
                self._scavenge_unlocked()
        except OSError as exc:
            logger.warning("Cannot scan cgroup root %s: %s", self.root, exc)

    def _scavenge_unlocked(self) -> None:
        for name in os.listdir(self.root):
            if not SLOT_DIR_RE.match(name):
                continue
            path = os.path.join(self.root, name)
            if not os.path.isdir(path):
                continue
            try:
                fd = _lock_directory(path, blocking=False)
            except BlockingIOError:
                # Its owner may be between create/place, or waitpid/destroy.
                continue
            except FileNotFoundError:
                continue
            try:
                procs = (_read(os.path.join(path, "cgroup.procs")) or "").split()
                if procs:
                    logger.warning(
                        "Leftover cgroup %s still has live process(es) %s; leaving it alone",
                        path,
                        ",".join(procs),
                    )
                    continue
                try:
                    os.rmdir(path)
                    logger.info("Removed leftover cgroup %s", path)
                except OSError as exc:
                    logger.warning("Cannot remove leftover cgroup %s: %s", path, exc)
            finally:
                os.close(fd)

    def create(self, role: str, index: int, limits: Limits) -> str:
        """Create the leaf cgroup and write its limits, before the fork.

        Writing limits up front means the child is already constrained the
        instant it is migrated in.
        """
        if (role, index) in self._paths:
            raise CgroupError(f"{role}[{index}] already owns a cgroup")
        path = self._claim_path(role, index)
        self._paths[(role, index)] = path
        try:
            # Swap first: capping swap after memory.max would leave a window
            # where the child can escape the limit by swapping out.
            if limits.memory_swap_max is not None:
                _write(
                    os.path.join(path, "memory.swap.max"), str(limits.memory_swap_max)
                )
            if limits.memory_high is not None:
                _write(os.path.join(path, "memory.high"), str(limits.memory_high))
            if limits.memory_max is not None:
                _write(os.path.join(path, "memory.max"), str(limits.memory_max))
            if limits.memory_oom_group is not None:
                _write(
                    os.path.join(path, "memory.oom.group"),
                    "1" if limits.memory_oom_group else "0",
                )
        except OSError as exc:
            self.destroy(role, index)
            raise CgroupError(f"cannot set up cgroup {path}: {exc}") from exc
        return path

    def place(self, pid: int, role: str, index: int) -> bool:
        """Migrate a freshly forked child into its leaf cgroup.

        Returns False when the child is already gone (ESRCH), which is not a
        cgroup failure — the normal reap path handles it.
        """
        slot_path = self.path_for(role, index)
        if slot_path is None:
            raise CgroupError(f"no cgroup created for {role}[{index}]")
        path = os.path.join(slot_path, "cgroup.procs")
        try:
            _write(path, str(pid))
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                return False
            raise CgroupError(f"cannot move pid {pid} into {path}: {exc}") from exc
        return True

    def stats(self, role: str, index: int) -> Optional[CgroupStats]:
        """Snapshot a child's cgroup. Must run before :meth:`destroy`.

        Each child gets a directory of its own, so the counters start at zero
        and any ``oom_kill`` is attributable to this child rather than to a
        predecessor in the same slot.
        """
        path = self.path_for(role, index)
        if path is None:
            return None
        events_raw = _read(os.path.join(path, "memory.events"))
        if events_raw is None:
            return None
        events = parse_keyed_file(events_raw)

        peak_raw = _read(os.path.join(path, "memory.peak"))
        peak = None
        if peak_raw is not None:
            try:
                peak = int(peak_raw.strip())
            except ValueError:
                peak = None

        max_raw = _read(os.path.join(path, "memory.max"))
        return CgroupStats(
            oom_kill=events.get("oom_kill", 0),
            oom_group_kill=events.get("oom_group_kill", 0),
            max_events=events.get("max", 0),
            memory_peak=peak,
            memory_max=max_raw.strip() if max_raw else None,
        )

    def destroy(self, role: str, index: int) -> None:
        """Remove a child's leaf cgroup, killing any surviving grandchildren.

        A job that forked a subprocess leaves the cgroup populated even after
        the worker itself is reaped; those are orphans now, so clearing them
        is both necessary to rmdir and the right thing to do.

        Only ever touches the directory this slot's child actually owned, so
        a co-existing predecessor is never disturbed.
        """
        path = self._paths.pop((role, index), None)
        fd = self._locks.pop((role, index), None)
        try:
            if path is not None:
                with _directory_lock(self.root):
                    self._destroy_path(path)
        except OSError as exc:
            logger.warning("Cannot remove cgroup %s: %s", path, exc)
        finally:
            if fd is not None:
                os.close(fd)

    def _destroy_path(self, path: str) -> None:
        if path is None or not os.path.isdir(path):
            return
        try:
            os.rmdir(path)
            return
        except OSError as exc:
            if exc.errno not in (errno.EBUSY, errno.ENOTEMPTY):
                logger.warning("Cannot remove cgroup %s: %s", path, exc)
                return

        self._kill_stragglers(path)
        for _ in range(5):
            try:
                os.rmdir(path)
                return
            except OSError as exc:
                if exc.errno not in (errno.EBUSY, errno.ENOTEMPTY):
                    logger.warning("Cannot remove cgroup %s: %s", path, exc)
                    return
        remaining = (_read(os.path.join(path, "cgroup.procs")) or "").split()
        logger.warning(
            "cgroup %s still busy (pids=%s); leaving it for the next startup sweep",
            path,
            ",".join(remaining) or "unknown",
        )

    def _kill_stragglers(self, path: str) -> None:
        kill_file = os.path.join(path, "cgroup.kill")
        if os.path.exists(kill_file):
            try:
                _write(kill_file, "1")
                return
            except OSError as exc:
                logger.warning("cgroup.kill on %s failed: %s", path, exc)
        for raw in (_read(os.path.join(path, "cgroup.procs")) or "").split():
            try:
                os.kill(int(raw), signal.SIGKILL)
            except (ValueError, ProcessLookupError):
                pass
            except OSError as exc:
                logger.warning(
                    "Cannot SIGKILL leftover pid %s in %s: %s", raw, path, exc
                )

    def __repr__(self) -> str:
        return (
            f"CgroupManager(root={self.root!r}, controllers={sorted(self.controllers)})"
        )


def probe(
    *,
    env=None,
    platform_name: Optional[str] = None,
    proc_mounts: str = "/proc/mounts",
    proc_self_cgroup: str = "/proc/self/cgroup",
):
    """Locate a usable, writable cgroup v2 subtree.

    Returns a :class:`CgroupManager` on success and a :class:`DisabledCgroup`
    carrying the reason otherwise. Never raises and never logs: the caller
    decides whether an unusable subtree is fatal, which depends on whether any
    limits were configured.
    """
    env = os.environ if env is None else env
    platform_name = sys.platform if platform_name is None else platform_name

    if env.get(ENABLE_ENV) == "0":
        return DisabledCgroup("disabled via QUEBEC_CGROUP=0")

    if not platform_name.startswith("linux"):
        return DisabledCgroup(f"not supported on {platform_name}")

    mounts = _read(proc_mounts)
    if mounts is None:
        return DisabledCgroup(f"cannot read {proc_mounts}")
    mountpoint = parse_cgroup2_mountpoint(mounts)
    if mountpoint is None:
        return DisabledCgroup(
            "no cgroup2 mount found; cgroup v1 / hybrid hierarchies are not supported"
        )

    explicit_root = env.get(ROOT_ENV)
    own_path = None
    raw_self = _read(proc_self_cgroup)
    if raw_self is not None:
        own_path = parse_proc_self_cgroup(raw_self)

    if explicit_root:
        root = explicit_root
        if not os.path.isdir(root):
            return DisabledCgroup(f"{ROOT_ENV}={root} is not a directory")
    else:
        if own_path is None:
            return DisabledCgroup(
                f"{proc_self_cgroup} has no cgroup v2 (0::) line; hybrid hierarchy?"
            )
        root = os.path.normpath(os.path.join(mountpoint, own_path.lstrip("/")))
        if not os.path.isdir(root):
            # cgroupns=host with the container's own cgroup mounted at the
            # mount point: the path from /proc/self/cgroup does not resolve.
            root = mountpoint

    # Migrating a process needs write access to the common ancestor of source
    # and destination cgroups. That holds automatically when we are inside the
    # root; outside it, only root can pull it off.
    own_full = (
        os.path.join(mountpoint, own_path.lstrip("/")) if own_path is not None else None
    )
    inside = own_full is not None and (
        os.path.normpath(own_full) == os.path.normpath(root)
        or os.path.normpath(own_full).startswith(os.path.normpath(root) + os.sep)
    )
    if not inside and os.geteuid() != 0:
        return DisabledCgroup(
            f"supervisor cgroup {own_full} is outside {root} and we are not root; "
            "cannot migrate into the target subtree"
        )

    for name in ("", "cgroup.procs", "cgroup.subtree_control"):
        target = os.path.join(root, name) if name else root
        if not os.access(target, os.W_OK):
            return DisabledCgroup(
                f"{target} is not writable; a delegated subtree is required "
                "(systemd Delegate=yes, root, or QUEBEC_CGROUP_ROOT)"
            )

    controllers = set((_read(os.path.join(root, "cgroup.controllers")) or "").split())
    if REQUIRED_CONTROLLER not in controllers:
        return DisabledCgroup(
            f"the '{REQUIRED_CONTROLLER}' controller is not available in {root} "
            f"(cgroup.controllers={' '.join(sorted(controllers)) or 'empty'})"
        )

    return CgroupManager(root, controllers={REQUIRED_CONTROLLER})

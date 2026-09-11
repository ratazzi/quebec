"""Unit tests for the cgroup v2 supervisor integration.

Everything here runs against a fake cgroup tree under ``tmp_path``, so the
whole file is platform-independent and runs on macOS. Real-kernel behaviour is
covered by the Docker suite under ``tests/docker/cgroup``.
"""

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import quebec
from quebec import cgroup
from quebec.cgroup import (
    EMPTY_LIMITS,
    CgroupError,
    CgroupManager,
    CgroupStats,
    DisabledCgroup,
    Limits,
)
from quebec.supervisor import (
    CHILD_OOM_SCORE_ADJ,
    ROLE_DISPATCHER,
    ROLE_WORKER,
    Supervisor,
    _ChildInfo,
    _ExitStatus,
    _SlotState,
    _reset_child_oom_priority,
    _write_oom_score_adj,
    classify_exit,
    current_supervisor,
    exceeded_own_limit,
    oom_failure_reason,
)


def make_root(tmp_path, *, controllers="cpuset cpu io memory pids", in_root=True):
    """Build a fake delegated cgroup root containing this process."""
    root = tmp_path / "root"
    root.mkdir()
    (root / "cgroup.controllers").write_text(controllers + "\n")
    (root / "cgroup.procs").write_text(f"{os.getpid()}\n" if in_root else "")
    (root / "cgroup.subtree_control").write_text("")
    return root


def make_proc_files(tmp_path, root, *, self_path="/root", mounts=None):
    mounts_file = tmp_path / "mounts"
    mounts_file.write_text(
        mounts
        if mounts is not None
        else f"cgroup2 {tmp_path} cgroup2 rw,nsdelegate 0 0\n"
    )
    self_file = tmp_path / "self_cgroup"
    self_file.write_text(f"0::{self_path}\n")
    return str(mounts_file), str(self_file)


class TestParsing:
    def test_proc_self_cgroup_v2_line(self):
        assert cgroup.parse_proc_self_cgroup("0::/system.slice/quebec.service") == (
            "/system.slice/quebec.service"
        )
        assert cgroup.parse_proc_self_cgroup("0::/") == "/"

    def test_proc_self_cgroup_rejects_v1_only(self):
        v1 = "12:memory:/user.slice\n11:cpu,cpuacct:/user.slice\n"
        assert cgroup.parse_proc_self_cgroup(v1) is None
        assert cgroup.parse_proc_self_cgroup("") is None

    def test_mountpoint_detection(self):
        pure_v2 = "cgroup2 /sys/fs/cgroup cgroup2 rw,nsdelegate 0 0\n"
        assert cgroup.parse_cgroup2_mountpoint(pure_v2) == "/sys/fs/cgroup"

        v1_only = (
            "tmpfs /sys/fs/cgroup tmpfs ro 0 0\n"
            "cgroup /sys/fs/cgroup/memory cgroup rw,memory 0 0\n"
        )
        assert cgroup.parse_cgroup2_mountpoint(v1_only) is None
        assert cgroup.parse_cgroup2_mountpoint("") is None

    def test_hybrid_picks_the_v2_mount(self):
        hybrid = (
            "cgroup /sys/fs/cgroup/memory cgroup rw,memory 0 0\n"
            "cgroup2 /sys/fs/cgroup/unified cgroup2 rw 0 0\n"
        )
        assert cgroup.parse_cgroup2_mountpoint(hybrid) == "/sys/fs/cgroup/unified"

    def test_keyed_file(self):
        text = "low 0\nhigh 0\nmax 3\noom 1\noom_kill 1\n"
        assert cgroup.parse_keyed_file(text) == {
            "low": 0,
            "high": 0,
            "max": 3,
            "oom": 1,
            "oom_kill": 1,
        }
        assert cgroup.parse_keyed_file("") == {}
        assert cgroup.parse_keyed_file("garbage\noom_kill notanumber\n") == {}

    @pytest.mark.parametrize(
        "raw,expected",
        [
            (1073741824, 1073741824),
            ("1073741824", 1073741824),
            ("1GiB", 1073741824),
            ("1Gi", 1073741824),
            ("1G", 1073741824),
            ("512MiB", 536870912),
            ("512M", 536870912),
            ("1GB", 1000000000),
            ("0", 0),
            ("max", None),
            ("abc", None),
            ("", None),
            (None, None),
        ],
    )
    def test_size_parsing(self, raw, expected):
        assert cgroup.parse_size_bytes(raw) == expected


class TestDerivation:
    def test_explicit_always_wins(self):
        assert cgroup.resolve_memory_max(999, 400 * 1024 * 1024) == (999, False)

    def test_derives_from_rss_rounded_up_to_mib(self):
        value, derived = cgroup.resolve_memory_max(None, 400 * 1024 * 1024)
        assert derived is True
        assert value == 600 * 1024 * 1024

    def test_rounds_up_to_whole_mib(self):
        value, derived = cgroup.resolve_memory_max(None, 100 * 1024 * 1024 + 1)
        assert derived is True
        assert value % (1024 * 1024) == 0
        assert value == 151 * 1024 * 1024

    def test_no_inputs_means_no_limit(self):
        assert cgroup.resolve_memory_max(None, None) == (None, False)
        assert cgroup.resolve_memory_max(None, 0) == (None, False)

    def test_factor_is_configurable(self):
        value, _ = cgroup.resolve_memory_max(None, 100 * 1024 * 1024, factor=1.1)
        assert value == 110 * 1024 * 1024

    def test_limits_from_config_applies_swap_and_oom_defaults(self):
        limits = cgroup.limits_from_config({"memory_max": 1024}, derive=True)
        assert limits.memory_max == 1024
        assert limits.memory_swap_max == 0
        assert limits.memory_oom_group is True

    def test_limits_from_config_respects_explicit_swap_and_oom(self):
        limits = cgroup.limits_from_config(
            {"memory_max": 1024, "memory_swap_max": 4096, "memory_oom_group": False},
            derive=True,
        )
        assert limits.memory_swap_max == 4096
        assert limits.memory_oom_group is False

    def test_no_derivation_when_cgroup_unavailable(self):
        entry = {"worker_max_rss_bytes": 400 * 1024 * 1024}
        assert cgroup.limits_from_config(entry, derive=False).memory_max is None
        assert cgroup.limits_from_config(entry, derive=True).memory_max is not None

    def test_explicit_max_never_derives(self):
        entry = {
            "memory_max": cgroup.UNLIMITED,
            "worker_max_rss_bytes": 100 * 1024 * 1024,
        }
        limits = cgroup.limits_from_config(entry, derive=True)
        assert limits.memory_max == cgroup.UNLIMITED
        assert limits.derived is False

    def test_explicit_max_does_not_pull_in_swap_and_oom_defaults(self):
        limits = cgroup.limits_from_config(
            {"memory_max": cgroup.UNLIMITED}, derive=True
        )
        assert limits.memory_swap_max is None, "an explicit max must not cap swap"
        assert limits.memory_oom_group is None

    def test_explicit_swap_max_is_written_through_not_zeroed(self):
        limits = cgroup.limits_from_config(
            {"memory_max": 1024, "memory_swap_max": cgroup.UNLIMITED}, derive=True
        )
        assert limits.memory_swap_max == cgroup.UNLIMITED

    def test_swap_and_oom_untouched_without_a_memory_limit(self):
        limits = cgroup.limits_from_config({"memory_high": 512}, derive=True)
        assert limits.memory_max is None
        assert limits.memory_swap_max is None
        assert limits.memory_oom_group is None


class TestConfiguredLimitDetection:
    def test_detects_any_explicit_field(self):
        for field in (
            "memory_max",
            "memory_high",
            "memory_swap_max",
            "memory_oom_group",
        ):
            raw = {"worker": [{field: 1}]}
            assert cgroup.config_has_explicit_limits(raw) is True

    def test_explicit_max_is_not_a_limit_that_must_be_enforced(self):
        raw = {"worker": [{"memory_max": cgroup.UNLIMITED}]}
        assert cgroup.config_has_explicit_limits(raw) is False

    def test_rss_alone_is_not_a_configured_limit(self):
        raw = {
            "worker": [{"worker_max_rss_bytes": 1024}],
            "default_worker_max_rss_bytes": 1024,
        }
        assert cgroup.config_has_explicit_limits(raw) is False

    def test_empty_inputs(self):
        assert cgroup.config_has_explicit_limits(None) is False
        assert cgroup.config_has_explicit_limits({}) is False
        assert cgroup.config_has_explicit_limits({"worker": []}) is False


class TestProbe:
    def test_non_linux_is_disabled(self, tmp_path):
        result = cgroup.probe(env={}, platform_name="darwin")
        assert isinstance(result, DisabledCgroup)
        assert "darwin" in result.reason

    def test_explicit_off_switch(self):
        result = cgroup.probe(env={"QUEBEC_CGROUP": "0"}, platform_name="linux")
        assert isinstance(result, DisabledCgroup)
        assert "QUEBEC_CGROUP=0" in result.reason

    def test_v1_only_is_rejected(self, tmp_path):
        root = make_root(tmp_path)
        mounts, self_file = make_proc_files(
            tmp_path,
            root,
            mounts="cgroup /sys/fs/cgroup/memory cgroup rw,memory 0 0\n",
        )
        result = cgroup.probe(
            env={"QUEBEC_CGROUP_ROOT": str(root)},
            platform_name="linux",
            proc_mounts=mounts,
            proc_self_cgroup=self_file,
        )
        assert isinstance(result, DisabledCgroup)
        assert "not supported" in result.reason

    def test_happy_path(self, tmp_path):
        root = make_root(tmp_path)
        mounts, self_file = make_proc_files(tmp_path, root)
        result = cgroup.probe(
            env={"QUEBEC_CGROUP_ROOT": str(root)},
            platform_name="linux",
            proc_mounts=mounts,
            proc_self_cgroup=self_file,
        )
        assert isinstance(result, CgroupManager)
        assert result.root == str(root)

    def test_missing_memory_controller(self, tmp_path):
        root = make_root(tmp_path, controllers="cpu io pids")
        mounts, self_file = make_proc_files(tmp_path, root)
        result = cgroup.probe(
            env={"QUEBEC_CGROUP_ROOT": str(root)},
            platform_name="linux",
            proc_mounts=mounts,
            proc_self_cgroup=self_file,
        )
        assert isinstance(result, DisabledCgroup)
        assert "'memory' controller" in result.reason

    def test_read_only_subtree(self, tmp_path):
        root = make_root(tmp_path)
        (root / "cgroup.subtree_control").chmod(0o444)
        mounts, self_file = make_proc_files(tmp_path, root)
        result = cgroup.probe(
            env={"QUEBEC_CGROUP_ROOT": str(root)},
            platform_name="linux",
            proc_mounts=mounts,
            proc_self_cgroup=self_file,
        )
        assert isinstance(result, DisabledCgroup)
        assert "not writable" in result.reason

    def test_explicit_root_that_does_not_exist(self, tmp_path):
        root = make_root(tmp_path)
        mounts, self_file = make_proc_files(tmp_path, root)
        result = cgroup.probe(
            env={"QUEBEC_CGROUP_ROOT": str(tmp_path / "nope")},
            platform_name="linux",
            proc_mounts=mounts,
            proc_self_cgroup=self_file,
        )
        assert isinstance(result, DisabledCgroup)
        assert "not a directory" in result.reason

    def test_derived_root_falls_back_to_mountpoint(self, tmp_path):
        """cgroupns=host: the path from /proc/self/cgroup does not resolve."""
        (tmp_path / "cgroup.controllers").write_text("memory pids\n")
        (tmp_path / "cgroup.procs").write_text(f"{os.getpid()}\n")
        (tmp_path / "cgroup.subtree_control").write_text("")
        mounts_file = tmp_path / "mounts"
        mounts_file.write_text(f"cgroup2 {tmp_path} cgroup2 rw 0 0\n")
        self_file = tmp_path / "self_cgroup"
        self_file.write_text("0::/some/host/path/that/does/not/exist\n")

        result = cgroup.probe(
            env={},
            platform_name="linux",
            proc_mounts=str(mounts_file),
            proc_self_cgroup=str(self_file),
        )
        assert isinstance(result, CgroupManager)
        assert result.root == str(tmp_path)


class TestManagerFileOperations:
    def manager(self, tmp_path):
        root = make_root(tmp_path)
        mgr = CgroupManager(str(root))
        mgr.prepare()
        return mgr, root

    def slot(self, root, name):
        return root / "workers" / name

    def test_prepare_moves_self_out_then_enables_controllers(self, tmp_path):
        root = make_root(tmp_path)
        CgroupManager(str(root)).prepare()

        # The supervisor goes into its own leaf, leaving `control` processless
        # so the memory controller can be enabled for the control slots.
        supervisor = root / "control" / "supervisor"
        assert supervisor.is_dir()
        assert supervisor.joinpath("cgroup.procs").read_text() == str(os.getpid())
        assert (root / "cgroup.subtree_control").read_text() == "+memory"
        assert (root / "control" / "cgroup.subtree_control").read_text() == "+memory"
        assert (root / "workers" / "cgroup.subtree_control").read_text() == "+memory"

    def test_prepare_is_idempotent(self, tmp_path):
        root = make_root(tmp_path)
        mgr = CgroupManager(str(root))
        mgr.prepare()
        (root / "cgroup.subtree_control").write_text("memory")
        mgr.prepare()
        # Already enabled: nothing appended, no EBUSY-triggering rewrite.
        assert (root / "cgroup.subtree_control").read_text() == "memory"

    def test_prepare_reports_other_processes_in_the_root(self, tmp_path):
        """EBUSY from the kernel is useless on its own; name the offenders."""
        root = make_root(tmp_path)
        (root / "cgroup.procs").write_text(f"{os.getpid()}\n999\n1234\n")

        with pytest.raises(CgroupError) as excinfo:
            CgroupManager(str(root)).prepare()

        message = str(excinfo.value)
        assert "999" in message and "1234" in message
        assert str(os.getpid()) not in message, "our own pid is not an offender"
        assert "QUEBEC_CGROUP_ROOT" in message
        # We still vacated our own pid before giving up.
        assert (root / "control" / "supervisor" / "cgroup.procs").read_text() == str(
            os.getpid()
        )

    def test_prepare_skips_migration_when_not_in_root(self, tmp_path):
        root = make_root(tmp_path, in_root=False)
        CgroupManager(str(root)).prepare()
        # Nothing to migrate, but the control children still need somewhere to
        # be placed, so the subtree itself is stood up regardless.
        assert not (root / "control" / "supervisor").exists()
        assert (root / "control" / "cgroup.subtree_control").read_text() == "+memory"

    def test_create_writes_limits_in_order(self, tmp_path):
        mgr, root = self.manager(tmp_path)
        limits = Limits(
            memory_max=128 * 1024 * 1024,
            memory_high=100 * 1024 * 1024,
            memory_swap_max=0,
            memory_oom_group=True,
        )
        path = mgr.create("worker", 0, limits)

        slot = self.slot(root, "worker-0")
        assert str(slot) == path
        assert slot.joinpath("memory.max").read_text() == str(128 * 1024 * 1024)
        assert slot.joinpath("memory.high").read_text() == str(100 * 1024 * 1024)
        assert slot.joinpath("memory.swap.max").read_text() == "0"
        assert slot.joinpath("memory.oom.group").read_text() == "1"

    def test_create_steps_over_an_occupied_directory(self, tmp_path):
        """A predecessor still draining must not have its limits rewritten."""
        mgr, root = self.manager(tmp_path)
        occupied = self.slot(root, "worker-0")
        occupied.mkdir()
        occupied.joinpath("cgroup.procs").write_text("4321\n")
        occupied.joinpath("memory.max").write_text("999\n")

        path = mgr.create("worker", 0, Limits(memory_max=128))

        assert path == str(self.slot(root, "worker-0.2"))
        assert occupied.joinpath("memory.max").read_text() == "999\n", (
            "the occupied cgroup's limits must be left alone"
        )
        assert self.slot(root, "worker-0.2").joinpath("memory.max").read_text() == "128"

    def test_an_existing_empty_directory_is_never_reused(self, tmp_path):
        """Empty is not free: its owner may simply not have destroyed it yet."""
        mgr, root = self.manager(tmp_path)
        self.slot(root, "worker-0").mkdir()

        assert mgr.create("worker", 0, Limits()) == str(self.slot(root, "worker-0.2"))

    def test_claims_climb_past_every_existing_name(self, tmp_path):
        mgr, root = self.manager(tmp_path)
        self.slot(root, "worker-0").mkdir()
        self.slot(root, "worker-0.2").mkdir()

        assert mgr.create("worker", 0, Limits()) == str(self.slot(root, "worker-0.3"))

    def test_a_lost_mkdir_race_falls_through_to_the_next_name(
        self, tmp_path, monkeypatch
    ):
        """Two supervisors can call mkdir at the same instant; the loser moves on."""
        mgr, root = self.manager(tmp_path)
        real_mkdir = os.mkdir
        calls = []

        def racing_mkdir(path, *args, **kwargs):
            # Only race on worker-leaf candidates; let the pool directory's
            # makedirs() pass through to the real syscall.
            if not os.path.basename(path).startswith("worker-"):
                return real_mkdir(path, *args, **kwargs)
            calls.append(path)
            if len(calls) == 1:
                # The other supervisor won this name between our two syscalls.
                raise FileExistsError(17, "File exists", path)
            return real_mkdir(path, *args, **kwargs)

        monkeypatch.setattr(os, "mkdir", racing_mkdir)

        assert mgr.create("worker", 0, Limits()) == str(self.slot(root, "worker-0.2"))
        assert calls == [
            str(self.slot(root, "worker-0")),
            str(self.slot(root, "worker-0.2")),
        ]

    def test_destroy_only_removes_the_directory_this_child_owned(self, tmp_path):
        mgr, root = self.manager(tmp_path)
        occupied = self.slot(root, "worker-0")
        occupied.mkdir()
        occupied.joinpath("cgroup.procs").write_text("4321\n")

        mgr.create("worker", 0, Limits())
        mgr.destroy("worker", 0)

        assert occupied.is_dir(), "the predecessor's cgroup must survive"
        assert not self.slot(root, "worker-0.2").exists()

    def test_place_and_stats_follow_the_claimed_path(self, tmp_path):
        mgr, root = self.manager(tmp_path)
        occupied = self.slot(root, "worker-0")
        occupied.mkdir()
        occupied.joinpath("cgroup.procs").write_text("4321\n")
        occupied.joinpath("memory.events").write_text("oom_kill 7\n")

        mgr.create("worker", 0, Limits())
        mgr.place(555, "worker", 0)

        assert self.slot(root, "worker-0.2").joinpath("cgroup.procs").read_text() == "555"
        assert occupied.joinpath("cgroup.procs").read_text() == "4321\n"
        # Counters come from the new directory, so they start at zero.
        assert mgr.stats("worker", 0) is None

    def test_a_free_directory_is_reused_after_the_child_is_gone(self, tmp_path):
        mgr, root = self.manager(tmp_path)
        mgr.create("worker", 0, Limits())
        mgr.destroy("worker", 0)
        assert mgr.create("worker", 0, Limits()) == str(self.slot(root, "worker-0"))

    def test_create_with_empty_limits_writes_nothing(self, tmp_path):
        mgr, root = self.manager(tmp_path)
        mgr.create("worker", 0, Limits())
        slot = self.slot(root, "worker-0")
        assert slot.is_dir()
        assert list(slot.iterdir()) == []

    def test_create_failure_raises_cgroup_error(self, tmp_path):
        mgr, root = self.manager(tmp_path)
        (root / "workers").chmod(0o555)
        try:
            with pytest.raises(CgroupError):
                mgr.create("worker", 0, Limits(memory_max=1024))
        finally:
            (root / "workers").chmod(0o755)

    def test_place_writes_pid(self, tmp_path):
        mgr, root = self.manager(tmp_path)
        mgr.create("worker", 1, Limits())
        assert mgr.place(4242, "worker", 1) is True
        assert self.slot(root, "worker-1").joinpath("cgroup.procs").read_text() == "4242"

    def test_place_missing_cgroup_raises(self, tmp_path):
        mgr, _ = self.manager(tmp_path)
        with pytest.raises(CgroupError):
            mgr.place(4242, "worker", 9)

    def test_stats_reads_events_peak_and_max(self, tmp_path):
        mgr, _root = self.manager(tmp_path)
        slot = Path(mgr.create("worker", 0, Limits()))
        slot.joinpath("memory.events").write_text(
            "low 0\nhigh 0\nmax 5\noom 1\noom_kill 1\noom_group_kill 1\n"
        )
        slot.joinpath("memory.peak").write_text("537067520\n")
        slot.joinpath("memory.max").write_text("536870912\n")

        stats = mgr.stats("worker", 0)
        assert stats == CgroupStats(
            oom_kill=1,
            oom_group_kill=1,
            max_events=5,
            memory_peak=537067520,
            memory_max="536870912",
        )

    def test_stats_returns_none_when_never_created(self, tmp_path):
        mgr, _ = self.manager(tmp_path)
        assert mgr.stats("worker", 0) is None

    def test_stats_tolerates_missing_peak(self, tmp_path):
        """memory.peak only exists on newer kernels."""
        mgr, _root = self.manager(tmp_path)
        slot = Path(mgr.create("worker", 0, Limits()))
        slot.joinpath("memory.events").write_text("oom_kill 2\n")

        stats = mgr.stats("worker", 0)
        assert stats.oom_kill == 2
        assert stats.memory_peak is None
        assert stats.memory_max is None

    def test_destroy_removes_the_directory(self, tmp_path):
        mgr, root = self.manager(tmp_path)
        mgr.create("worker", 0, Limits())
        mgr.destroy("worker", 0)
        assert not self.slot(root, "worker-0").exists()

    def test_destroy_is_a_noop_when_already_gone(self, tmp_path):
        mgr, _ = self.manager(tmp_path)
        mgr.destroy("worker", 0)

    def test_control_roles_get_their_own_leaf_under_control(self, tmp_path):
        """A dispatcher limit in queue.yml has to reach a real cgroup file."""
        mgr, root = self.manager(tmp_path)
        path = mgr.create("dispatcher", 0, Limits(memory_max=1024))

        leaf = root / "control" / "dispatcher-0"
        assert path == str(leaf)
        assert leaf.joinpath("memory.max").read_text() == "1024"
        # Pooling is a worker-only affair: control slots sit beside the
        # supervisor, outside the workers budget.
        assert not (root / "workers" / "dispatcher-0").exists()

    def test_place_control_role_uses_its_own_leaf(self, tmp_path):
        mgr, root = self.manager(tmp_path)
        mgr.create("scheduler", 0, Limits())
        assert mgr.place(4242, "scheduler", 0) is True

        assert (root / "control" / "scheduler-0" / "cgroup.procs").read_text() == "4242"
        # Never the shared parent: a process there would block the controller.
        parent_procs = root / "control" / "cgroup.procs"
        assert not parent_procs.exists() or parent_procs.read_text().strip() != "4242"

    def test_destroy_removes_a_control_leaf_but_never_the_parent(self, tmp_path):
        mgr, root = self.manager(tmp_path)
        mgr.create("scheduler", 0, Limits())

        mgr.destroy("scheduler", 0)

        assert not (root / "control" / "scheduler-0").exists()
        assert (root / "control").is_dir()

    def test_prepare_writes_pool_limits(self, tmp_path):
        root = make_root(tmp_path)
        mgr = CgroupManager(
            str(root),
            workers_pool_limits=Limits(memory_max=7 * 1024**3, memory_oom_group=False),
        )
        mgr.prepare()

        workers = root / "workers"
        assert workers.joinpath("memory.max").read_text() == str(7 * 1024**3)
        assert workers.joinpath("memory.oom.group").read_text() == "0"


class TestScavenge:
    def test_reclaims_after_owner_process_exits(self, tmp_path):
        root = make_root(tmp_path)
        subprocess.run(
            [
                sys.executable,
                "-c",
                """
import os, sys
from quebec.cgroup import CgroupManager, Limits
manager = CgroupManager(sys.argv[1])
manager.create("worker", 0, Limits())
os._exit(0)  # Simulate owner death without running Python cleanup.
""",
                str(root),
            ],
            check=True,
            timeout=10,
        )
        CgroupManager(str(root)).scavenge()
        assert not (root / "workers" / "worker-0").exists()

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is Unix-only")
    def test_child_close_preserves_parent_lock_without_retaining_it(self, tmp_path):
        root = make_root(tmp_path)
        subprocess.run(
            [
                sys.executable,
                "-c",
                """
import os, sys
from quebec.cgroup import CgroupManager, Limits
manager = CgroupManager(sys.argv[1])
path = manager.create("worker", 0, Limits())
ready_r, ready_w = os.pipe()
stop_r, stop_w = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(ready_r)
    os.close(stop_w)
    manager.close()
    os.write(ready_w, b"x")
    os.read(stop_r, 1)
    os._exit(0)
os.close(ready_w)
os.close(stop_r)
try:
    assert os.read(ready_r, 1) == b"x"
    scanner = CgroupManager(sys.argv[1])
    scanner.scavenge()
    assert os.path.isdir(path), "child close unlocked its parent"
    manager.close()
    scanner.scavenge()
    assert not os.path.exists(path), "child retained an inherited lock"
finally:
    os.close(ready_r)
    os.close(stop_w)
    os.waitpid(pid, 0)
""",
                str(root),
            ],
            check=True,
            timeout=10,
        )

    def test_preserves_another_supervisors_unplaced_child(self, tmp_path):
        root = make_root(tmp_path)
        first = CgroupManager(str(root))
        second = CgroupManager(str(root))
        first_path = first.create(ROLE_WORKER, 0, Limits())
        try:
            second.scavenge()
            second_path = second.create(ROLE_WORKER, 0, Limits())
            assert first_path != second_path
            assert Path(first_path).is_dir()
        finally:
            first.destroy(ROLE_WORKER, 0)
            second.destroy(ROLE_WORKER, 0)

    def test_removes_empty_slot_dirs_only(self, tmp_path):
        root = make_root(tmp_path)
        (root / "workers").mkdir()
        # Left without a cgroup.procs file: a real hierarchy removes the
        # interface files as part of rmdir, which a plain directory cannot
        # model. The decision under test is "no live pids -> remove".
        for name in ("worker-0", "worker-0.2"):
            (root / "workers" / name).mkdir()

        busy = root / "workers" / "worker-7"
        busy.mkdir()
        busy.joinpath("cgroup.procs").write_text("1234\n")

        # Control slots are leftovers too, but the supervisor's own leaf and
        # anything that is not a slot name must survive.
        (root / "control").mkdir()
        for name in ("dispatcher-1", "scheduler-0", "supervisor", "not-a-slot"):
            (root / "control" / name).mkdir()
        for name in ("some-other-thing", "worker-abc", "dispatcher-1"):
            (root / name).mkdir()

        CgroupManager(str(root)).scavenge()

        assert not (root / "workers" / "worker-0").exists()
        assert not (root / "workers" / "worker-0.2").exists()
        assert busy.exists(), "a populated leftover must be left for its owner"
        assert not (root / "control" / "dispatcher-1").exists()
        assert not (root / "control" / "scheduler-0").exists()
        assert (root / "control" / "supervisor").exists(), (
            "the supervisor's own leaf is never a scavenge candidate"
        )
        assert (root / "control" / "not-a-slot").exists()
        # Only the two subtrees are scanned; the root itself is left alone.
        for name in ("some-other-thing", "worker-abc", "dispatcher-1"):
            assert (root / name).exists()

    def test_empty_procs_file_is_not_treated_as_busy(self, tmp_path, caplog):
        root = make_root(tmp_path)
        (root / "workers").mkdir()
        slot = root / "workers" / "worker-0"
        slot.mkdir()
        slot.joinpath("cgroup.procs").write_text("")

        CgroupManager(str(root)).scavenge()

        assert "still has live process" not in caplog.text

    def test_missing_root_only_warns(self, tmp_path):
        CgroupManager(str(tmp_path / "gone")).scavenge()


class TestDisabledCgroup:
    def test_every_method_is_a_noop(self, tmp_path):
        disabled = DisabledCgroup("because")
        assert disabled.enabled is False
        assert disabled.prepare() is None
        assert disabled.scavenge() is None
        assert disabled.create("worker", 0, Limits(memory_max=1)) is None
        assert disabled.place(1, "worker", 0) is True
        assert disabled.stats("worker", 0) is None
        assert disabled.destroy("worker", 0) is None
        assert disabled.path_for("worker", 0) is None
        assert list(tmp_path.iterdir()) == []


class TestExitClassification:
    def exited(self, code):
        return _ExitStatus(exited=True, exit_code=code, signaled=False, signal=None)

    def signaled(self, sig):
        return _ExitStatus(exited=False, exit_code=None, signaled=True, signal=sig)

    def test_planned_recycle_wins_over_everything(self):
        assert classify_exit(self.exited(75), None, False) == "planned_recycle"
        assert (
            classify_exit(self.exited(75), CgroupStats(oom_kill=1), False)
            == "planned_recycle"
        )

    def test_oom_detected_from_the_counter(self):
        status = self.signaled(9)
        assert classify_exit(status, CgroupStats(oom_kill=1), False) == "oom"

    def test_oom_counter_outranks_our_own_sigkill(self):
        status = self.signaled(9)
        assert classify_exit(status, CgroupStats(oom_kill=1), True) == "oom"

    def test_our_sigkill_without_oom(self):
        status = self.signaled(9)
        assert (
            classify_exit(status, CgroupStats(oom_kill=0), True)
            == "killed_by_supervisor"
        )

    def test_foreign_sigkill_without_cgroup_info(self):
        assert classify_exit(self.signaled(9), None, False) == "signaled"

    def test_plain_crash(self):
        assert classify_exit(self.exited(1), CgroupStats(), False) == "crashed"
        assert classify_exit(self.exited(0), None, False) == "crashed"

    def test_segfault_is_not_an_oom(self):
        assert classify_exit(self.signaled(11), CgroupStats(oom_kill=0), False) == (
            "signaled"
        )


class FakeQuebec:
    """Just enough Quebec for Supervisor construction and reap bookkeeping."""

    def __init__(self):
        self.failed = []

    def supervisor_resource_limits_from_config(self):
        return {}

    def supervisor_fail_claimed_by_pid(self, pid, hostname, reason=None):
        self.failed.append((pid, reason))
        return 0


class EnabledNoopCgroup(DisabledCgroup):
    """A backend that reports itself usable but does nothing.

    ``DisabledCgroup`` doubles as the no-op implementation; flipping ``enabled``
    lets a test exercise the paths that only run when a cgroup is available.
    """

    enabled = True


class ExplodingCgroup(DisabledCgroup):
    """Cgroup backend whose create() starts failing after ``after`` calls."""

    enabled = True

    def __init__(self, after):
        super().__init__("test double")
        self.after = after
        self.calls = 0

    def create(self, role, index, limits):
        self.calls += 1
        if self.calls > self.after:
            raise CgroupError(f"boom on call {self.calls}")


def make_supervisor(cgroup, plan=None, limited_slots=(), **kwargs):
    qc = FakeQuebec()
    sup = Supervisor(qc, plan or {ROLE_WORKER: 2}, cgroup=cgroup, **kwargs)
    sup._cgroup_ready = True
    # A slot with no configured limit is best-effort by design, so the
    # failure paths under test only trigger where a limit was asked for.
    for key in limited_slots:
        sup._slot_limits[key] = Limits(memory_max=1024)
    return sup, qc


class TestRuntimeCgroupFailureIsSlotLocal:
    """A refork that cannot get its cgroup must not stop the whole fleet."""

    def test_startup_failure_stays_fatal(self, tmp_path):
        sup, _qc = make_supervisor(
            ExplodingCgroup(after=0), limited_slots=[(ROLE_WORKER, 0)]
        )
        assert sup._starting is True
        with pytest.raises(CgroupError):
            sup._fork_child(ROLE_WORKER, 0)

    def test_runtime_failure_only_marks_the_slot(self, tmp_path):
        sup, _qc = make_supervisor(
            ExplodingCgroup(after=0),
            crash_loop_max=3,
            limited_slots=[(ROLE_WORKER, 0)],
        )
        sup._starting = False
        sup._slots[(ROLE_WORKER, 0)] = _SlotState()
        sup._slots[(ROLE_WORKER, 1)] = _SlotState()

        sup._fork_child(ROLE_WORKER, 0)

        assert sup._children == {}, "no child was forked"
        assert len(sup._slots[(ROLE_WORKER, 0)].crash_times) == 1
        assert sup._slots[(ROLE_WORKER, 0)].disabled is False
        assert (ROLE_WORKER, 0) in sup._pending_forks
        assert sup._slots[(ROLE_WORKER, 1)].crash_times == [], "other slots untouched"

    def test_repeated_runtime_failure_disables_just_that_slot(self, tmp_path):
        sup, _qc = make_supervisor(
            ExplodingCgroup(after=0),
            crash_loop_max=2,
            limited_slots=[(ROLE_WORKER, 0)],
        )
        sup._starting = False
        sup._slots[(ROLE_WORKER, 0)] = _SlotState()
        sup._slots[(ROLE_WORKER, 1)] = _SlotState()

        sup._fork_child(ROLE_WORKER, 0)
        sup._fork_child(ROLE_WORKER, 0)

        assert sup._slots[(ROLE_WORKER, 0)].disabled is True
        assert (ROLE_WORKER, 0) not in sup._pending_forks, "a dead slot stops retrying"
        assert sup._slots[(ROLE_WORKER, 1)].disabled is False

    def test_pending_forks_are_retried_and_dropped_when_disabled(self, tmp_path):
        sup, _qc = make_supervisor(
            ExplodingCgroup(after=0),
            crash_loop_max=99,
            limited_slots=[(ROLE_WORKER, 0)],
        )
        sup._starting = False
        sup._slots[(ROLE_WORKER, 0)] = _SlotState(disabled=True)
        sup._pending_forks.add((ROLE_WORKER, 0))

        sup._retry_pending_forks()

        assert sup._pending_forks == set()


class TestShutdownKeepsOomAttribution:
    """P2-4: the drain path reaps children too, and must attribute the same."""

    def stats_for(self, tmp_path, **fields):
        root = make_root(tmp_path)
        mgr = CgroupManager(str(root))
        slot = Path(mgr.create(ROLE_WORKER, 0, Limits()))
        slot.joinpath("memory.events").write_text(
            f"max {fields.get('max_events', 0)}\noom_kill {fields.get('oom_kill', 0)}\n"
        )
        slot.joinpath("memory.peak").write_text(f"{fields.get('peak', 0)}\n")
        slot.joinpath("memory.max").write_text(f"{fields.get('limit', 'max')}\n")
        return mgr

    def test_reclaim_attributes_an_oom_during_shutdown(self, tmp_path):
        mgr = self.stats_for(
            tmp_path, oom_kill=1, max_events=1, peak=134217728, limit=134217728
        )
        sup, qc = make_supervisor(mgr, plan={ROLE_WORKER: 1})
        info = _ChildInfo(role=ROLE_WORKER, index=0, pid=99)

        kind, stats = sup._reclaim_child(
            99, info, _ExitStatus(False, None, True, signal.SIGKILL)
        )

        assert kind == "oom"
        assert stats.oom_kill == 1
        assert len(qc.failed) == 1
        pid, reason = qc.failed[0]
        assert pid == 99
        assert "killed by the OOM killer" in reason
        assert "Likely exceeded this worker" in reason

    def test_reap_until_uses_the_same_path(self, tmp_path):
        """_reap_until must not bypass the attribution that _handle_exit does."""
        mgr = self.stats_for(tmp_path, oom_kill=1, max_events=1, limit=64)
        sup, qc = make_supervisor(mgr, plan={ROLE_WORKER: 1})
        info = _ChildInfo(role=ROLE_WORKER, index=0, pid=77)
        sup._children[77] = info
        sup._reap_one = lambda *, block: (
            (77, _ExitStatus(False, None, True, signal.SIGKILL))
            if 77 in sup._children
            else (0, None)
        )

        sup._reap_until(deadline=time.monotonic() + 1.0)

        assert [r for _pid, r in qc.failed if r and "OOM killer" in r], (
            "the drain path recorded a plain crash instead of an OOM"
        )

    def test_ordinary_shutdown_exit_is_not_mislabelled(self, tmp_path):
        mgr = self.stats_for(tmp_path)
        sup, qc = make_supervisor(mgr, plan={ROLE_WORKER: 1})
        info = _ChildInfo(role=ROLE_WORKER, index=0, pid=42)

        kind, _stats = sup._reclaim_child(42, info, _ExitStatus(True, 0, False, None))

        assert kind == "crashed"
        assert qc.failed == [(42, None)]


class TestSupervisorConfigAndStartup:
    @pytest.mark.parametrize("disabled_value", ["max", "0"])
    def test_disabled_recycle_survives_config_export(
        self, tmp_path, monkeypatch, disabled_value
    ):
        config = tmp_path / "queue.yml"
        config.write_text(
            "test:\n  workers:\n    - processes: 1\n"
            f"      memory_recycle_at: {disabled_value}\n"
        )
        monkeypatch.setenv("QUEBEC_CONFIG", str(config))
        monkeypatch.setenv("QUEBEC_ENV", "test")
        qc = quebec.Quebec("sqlite::memory:", worker_max_rss_mb=100)
        try:
            raw = qc.supervisor_resource_limits_from_config()
            assert raw[ROLE_WORKER][0]["worker_max_rss_bytes"] is None
            sup = Supervisor(
                qc, {ROLE_WORKER: 1}, cgroup=CgroupManager(str(tmp_path)), limits=raw
            )
            assert sup._slot_limits[ROLE_WORKER, 0].memory_max is None
        finally:
            qc.close()

    def test_pool_budget_comes_from_queue_yml(self, tmp_path, monkeypatch):
        """The ordinary entrypoint reads the budget through the config bridge."""
        config = tmp_path / "queue.yml"
        config.write_text(
            "test:\n  workers_pool_memory_max: 7GiB\n  workers:\n    - processes: 1\n"
        )
        monkeypatch.setenv("QUEBEC_CONFIG", str(config))
        monkeypatch.setenv("QUEBEC_ENV", "test")
        qc = quebec.Quebec("sqlite::memory:")
        try:
            raw = qc.supervisor_resource_limits_from_config()
            assert raw["workers_pool_memory_max"] == 7 * 1024**3
            sup = Supervisor(
                qc, {ROLE_WORKER: 1}, cgroup=CgroupManager(str(tmp_path)), limits=raw
            )
            assert sup._workers_pool_limits.memory_max == 7 * 1024**3
        finally:
            qc.close()

    def test_pool_budget_falls_back_to_the_environment(self, monkeypatch):
        monkeypatch.setenv("QUEBEC_WORKERS_POOL_MEMORY_MAX", "512MiB")
        sup, _qc = make_supervisor(EnabledNoopCgroup("t"), plan={ROLE_WORKER: 1})
        assert sup._workers_pool_limits.memory_max == 512 * 1024 * 1024

    def test_queue_yml_outranks_the_environment(self, monkeypatch):
        monkeypatch.setenv("QUEBEC_WORKERS_POOL_MEMORY_MAX", "512MiB")
        sup, _qc = make_supervisor(
            EnabledNoopCgroup("t"),
            plan={ROLE_WORKER: 1},
            limits={"workers_pool_memory_max": 2 * 1024**3},
        )
        assert sup._workers_pool_limits.memory_max == 2 * 1024**3

    def test_the_constructor_outranks_both(self, monkeypatch):
        monkeypatch.setenv("QUEBEC_WORKERS_POOL_MEMORY_MAX", "512MiB")
        sup, _qc = make_supervisor(
            EnabledNoopCgroup("t"),
            plan={ROLE_WORKER: 1},
            limits={"workers_pool_memory_max": 2 * 1024**3},
            workers_pool_memory_max="1GiB",
        )
        assert sup._workers_pool_limits.memory_max == 1024**3

    def test_an_empty_environment_value_is_not_a_budget(self, monkeypatch):
        monkeypatch.setenv("QUEBEC_WORKERS_POOL_MEMORY_MAX", "")
        sup, _qc = make_supervisor(EnabledNoopCgroup("t"), plan={ROLE_WORKER: 1})
        assert sup._workers_pool_limits is cgroup.EMPTY_LIMITS

    def test_initial_placement_failure_aborts_and_reclaims_child(self, monkeypatch):
        qc = MagicMock()
        qc.register_supervisor.return_value = 1
        cg = MagicMock(enabled=True)
        cg.stats.return_value = None
        cg.place.side_effect = CgroupError("cannot migrate initial worker")
        sup = Supervisor(
            qc,
            {ROLE_WORKER: 1},
            cgroup=cg,
            limits={ROLE_WORKER: [{"memory_max": 1024}]},
        )
        monkeypatch.setattr(sup, "_install_signal_handlers", lambda: None)
        monkeypatch.setattr(sup, "_start_heartbeat", lambda: None)
        monkeypatch.setattr(sup, "_start_maintenance", lambda: None)
        monkeypatch.setattr(sup, "_supervise", lambda: None)
        monkeypatch.setattr(os, "fork", lambda: 424242)
        monkeypatch.setattr(os, "kill", lambda *_args: None)
        monkeypatch.setattr(
            sup,
            "_reap_one",
            lambda **_kwargs: (424242, _ExitStatus(True, 1, False, None)),
        )

        with pytest.raises(CgroupError, match="cannot migrate"):
            sup.start()

        qc.systemd_ready.assert_not_called()
        assert sup._children == {}
        cg.destroy.assert_called_once_with(ROLE_WORKER, 0)
        qc.deregister_process.assert_called_once_with(1)


class TestMustEnforce:
    """One judgement decides what is fatal, so the three paths cannot disagree."""

    def test_explicit_max_asks_the_kernel_for_nothing(self):
        assert Limits(memory_max=cgroup.UNLIMITED).must_enforce() is False
        assert Limits(memory_swap_max=cgroup.UNLIMITED).must_enforce() is False
        assert Limits().must_enforce() is False

    def test_numbers_and_oom_group_must_be_enforced(self):
        assert Limits(memory_max=1024).must_enforce() is True
        assert Limits(memory_high=1024).must_enforce() is True
        assert Limits(memory_swap_max=0).must_enforce() is True
        assert Limits(memory_oom_group=False).must_enforce() is True

    def test_probe_failure_with_only_max_is_not_fatal(self):
        raw = {"worker": [{"memory_max": cgroup.UNLIMITED}]}
        assert cgroup.config_has_explicit_limits(raw) is False
        # Construction must not raise even though a cgroup is unavailable.
        Supervisor(
            FakeQuebec(),
            {ROLE_WORKER: 1},
            cgroup=DisabledCgroup("no cgroup here"),
            limits=raw,
        )

    def test_prepare_failure_with_only_max_is_not_fatal(self, tmp_path):
        class BadPrepare(DisabledCgroup):
            enabled = True

            def prepare(self):
                raise CgroupError("cannot enable controllers")

        sup, _qc = make_supervisor(BadPrepare("test double"), plan={ROLE_WORKER: 1})
        sup._slot_limits[(ROLE_WORKER, 0)] = Limits(memory_max=cgroup.UNLIMITED)
        sup._cgroup_ready = False

        sup._setup_cgroup_root()

        assert sup._cgroup.enabled is False, "degraded instead of aborting"

    def test_create_failure_with_only_max_lets_the_child_run(self, tmp_path):
        sup, _qc = make_supervisor(ExplodingCgroup(after=0), plan={ROLE_WORKER: 1})
        sup._slot_limits[(ROLE_WORKER, 0)] = Limits(memory_max=cgroup.UNLIMITED)

        assert (
            sup._create_slot_cgroup(ROLE_WORKER, 0, Limits(memory_max="max")) is False
        )

    def test_place_failure_with_only_max_lets_the_child_run(self, tmp_path):
        class BadPlace(DisabledCgroup):
            enabled = True

            def place(self, pid, role, index):
                raise CgroupError("cannot migrate")

        sup, _qc = make_supervisor(BadPlace("test double"), plan={ROLE_WORKER: 1})

        released = sup._place_in_cgroup(
            123, ROLE_WORKER, 0, Limits(memory_max=cgroup.UNLIMITED)
        )

        assert released is True, "an unenforceable `max` must not kill the child"


class TestPoolBudgetIsEnforced:
    """The pool budget is a configured limit, so it decides what is fatal too.

    It arrives as a kwarg rather than through queue.yml, so every path that
    asks "can this cgroup failure be tolerated?" has to consult it separately
    or a worker ends up running outside the budget — in the supervisor's own
    cgroup, competing with the control processes it was meant to protect.
    """

    class BadPrepare(DisabledCgroup):
        enabled = True

        def prepare(self):
            raise CgroupError("cannot enable controllers")

    class BadPlace(DisabledCgroup):
        enabled = True

        def place(self, pid, role, index):
            raise CgroupError("cannot migrate")

    def test_an_unusable_cgroup_with_a_pool_budget_is_fatal(self):
        with pytest.raises(RuntimeError, match="workers pool memory budget"):
            Supervisor(
                FakeQuebec(),
                {ROLE_WORKER: 1},
                cgroup=DisabledCgroup("no cgroup here"),
                workers_pool_memory_max="7GiB",
            )

    def test_no_pool_budget_stays_best_effort(self):
        """The default must never turn a cgroup-less host into a hard error."""
        sup = Supervisor(
            FakeQuebec(), {ROLE_WORKER: 1}, cgroup=DisabledCgroup("no cgroup here")
        )
        assert sup._workers_pool_limits is cgroup.EMPTY_LIMITS
        assert sup._enforced_limit_sources() == []

    def test_an_explicit_max_pool_budget_asks_for_nothing(self):
        sup = Supervisor(
            FakeQuebec(),
            {ROLE_WORKER: 1},
            cgroup=DisabledCgroup("no cgroup here"),
            workers_pool_memory_max="max",
        )
        assert sup._workers_pool_limits.memory_max == cgroup.UNLIMITED
        assert sup._workers_pool_limits.memory_oom_group is None
        assert sup._enforced_limit_sources() == []

    def test_prepare_failure_with_a_pool_budget_is_fatal(self):
        sup, _qc = make_supervisor(
            self.BadPrepare("test double"),
            plan={ROLE_WORKER: 1},
            workers_pool_memory_max="7GiB",
        )
        sup._cgroup_ready = False

        with pytest.raises(RuntimeError, match="cannot prepare cgroup root"):
            sup._setup_cgroup_root()

    def test_create_failure_with_a_pool_budget_fails_the_slot(self):
        """The worker has no limit of its own; the budget alone must bind it."""
        sup, _qc = make_supervisor(
            ExplodingCgroup(after=0),
            plan={ROLE_WORKER: 1},
            workers_pool_memory_max="7GiB",
        )

        with pytest.raises(CgroupError):
            sup._create_slot_cgroup(ROLE_WORKER, 0, EMPTY_LIMITS)

    def test_place_failure_with_a_pool_budget_aborts_the_worker(self):
        sup, _qc = make_supervisor(
            self.BadPlace("test double"),
            plan={ROLE_WORKER: 1},
            workers_pool_memory_max="7GiB",
        )
        sup._starting = False

        released = sup._place_in_cgroup(123, ROLE_WORKER, 0, EMPTY_LIMITS)

        assert released is False, (
            "a worker outside the workers subtree runs in the supervisor's own "
            "cgroup, escaping the budget entirely"
        )

    def test_control_roles_are_not_bound_by_the_pool_budget(self):
        """The budget caps the worker pool; a dispatcher is not in it."""
        sup, _qc = make_supervisor(
            self.BadPlace("test double"),
            plan={ROLE_WORKER: 1, ROLE_DISPATCHER: 1},
            workers_pool_memory_max="7GiB",
        )
        sup._starting = False

        assert sup._place_in_cgroup(123, ROLE_DISPATCHER, 0, EMPTY_LIMITS) is True


class TestFailureReason:
    """memory.events counts kills by *any* OOM killer, the global one included,
    so the wording may only claim what the counters actually prove."""

    def test_includes_the_counters_it_has(self):
        reason = oom_failure_reason(
            4242,
            CgroupStats(
                oom_kill=1, max_events=1, memory_peak=537067520, memory_max="536870912"
            ),
        )
        assert "killed by the OOM killer" in reason
        assert "pid=4242" in reason
        assert "memory.max=536870912" in reason
        assert "memory.peak=537067520" in reason
        assert "oom_kill=1" in reason

    def test_blames_the_local_limit_only_with_evidence(self):
        hit_the_limit = CgroupStats(oom_kill=1, max_events=1, memory_max="536870912")
        assert exceeded_own_limit(hit_the_limit) is True
        assert "Likely exceeded this worker" in oom_failure_reason(1, hit_the_limit)

        peaked_at_the_limit = CgroupStats(
            oom_kill=1, memory_peak=536870912, memory_max="536870912"
        )
        assert exceeded_own_limit(peaked_at_the_limit) is True

    def test_does_not_blame_the_local_limit_without_one(self):
        """A global OOM kill in an unlimited cgroup: state the fact, nothing more."""
        no_limit = CgroupStats(oom_kill=1, memory_max="max")
        assert exceeded_own_limit(no_limit) is False
        reason = oom_failure_reason(7, no_limit)
        assert "killed by the OOM killer" in reason
        assert "Likely exceeded" not in reason

        unset = CgroupStats(oom_kill=1)
        assert exceeded_own_limit(unset) is False
        assert exceeded_own_limit(None) is False

    def test_does_not_blame_the_limit_when_well_under_it(self):
        under = CgroupStats(
            oom_kill=1, max_events=0, memory_peak=1024, memory_max="536870912"
        )
        assert exceeded_own_limit(under) is False
        assert "Likely exceeded" not in oom_failure_reason(1, under)

    def test_survives_missing_stats(self):
        reason = oom_failure_reason(1, None)
        assert "killed by the OOM killer" in reason
        assert "pid=1" in reason


class TestOomScoreAdj:
    """Protecting the supervisor is the unit's job; only workers give it up."""

    def test_write_targets_the_proc_interface(self, monkeypatch):
        fake = MagicMock()
        monkeypatch.setattr("builtins.open", fake)
        assert _write_oom_score_adj(4242, -1000) is True
        fake.assert_called_once_with("/proc/4242/oom_score_adj", "w")
        fake.return_value.__enter__.return_value.write.assert_called_once_with("-1000")

    def test_write_is_best_effort(self, monkeypatch):
        def boom(*_args, **_kwargs):
            raise OSError("no /proc here")

        monkeypatch.setattr("builtins.open", boom)
        assert _write_oom_score_adj(4242, 0) is False

    def test_the_supervisor_never_writes_its_own_score(self, monkeypatch):
        """Lowering it needs CAP_SYS_RESOURCE; the unit owns this, not us."""
        assert not hasattr(Supervisor, "_protect_from_oom")

        qc = MagicMock()
        qc.register_supervisor.return_value = 1
        writes = []
        monkeypatch.setattr(
            "quebec.supervisor._write_oom_score_adj",
            lambda pid, value: writes.append((pid, value)) or True,
        )
        sup = Supervisor(
            qc, {ROLE_WORKER: 1}, cgroup=DisabledCgroup("test"), limits={}
        )
        sup._stopping = True  # register, then unwind without forking
        sup.start()

        assert writes == []

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is Unix-only")
    def test_forked_child_resets_to_default(self, monkeypatch):
        read_fd, write_fd = os.pipe()

        def child_side_reset():
            os.write(write_fd, b"reset")
            os.close(write_fd)
            os._exit(0)  # short-circuit before user code runs

        monkeypatch.setattr(
            "quebec.supervisor._reset_child_oom_priority", child_side_reset
        )
        sup, _qc = make_supervisor(DisabledCgroup("test"), plan={ROLE_WORKER: 1})
        sup._fork_child(ROLE_WORKER, 0)

        (child_pid,) = sup._children
        os.close(write_fd)
        try:
            data = os.read(read_fd, 1024)
        finally:
            os.close(read_fd)
        os.waitpid(child_pid, 0)
        assert data == b"reset"

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is Unix-only")
    def test_a_forked_control_role_keeps_the_inherited_protection(self, monkeypatch):
        """Losing the dispatcher to a worker's memory spike is the failure
        this protection exists to prevent, so it is never dropped there."""
        read_fd, write_fd = os.pipe()

        def child_side_reset():
            os.write(write_fd, b"reset")
            os.close(write_fd)
            os._exit(0)

        monkeypatch.setattr(
            "quebec.supervisor._reset_child_oom_priority", child_side_reset
        )
        sup, qc = make_supervisor(
            DisabledCgroup("test"), plan={ROLE_DISPATCHER: 1}
        )
        qc.reset_after_fork = lambda: os._exit(0)  # stop before the role loop
        sup._fork_child(ROLE_DISPATCHER, 0)

        (child_pid,) = sup._children
        os.close(write_fd)
        try:
            data = os.read(read_fd, 1024)
        finally:
            os.close(read_fd)
        os.waitpid(child_pid, 0)
        assert data == b"", "a control role must not reset its oom_score_adj"

    def test_child_reset_skips_when_already_default(self, monkeypatch):
        class FakeProc:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return "0\n"

        monkeypatch.setattr("builtins.open", FakeProc)
        writes = []
        monkeypatch.setattr(
            "quebec.supervisor._write_oom_score_adj",
            lambda pid, value: writes.append((pid, value)) or True,
        )
        assert _reset_child_oom_priority() is True
        assert writes == []

    def test_child_reset_writes_when_inherited(self, monkeypatch):
        class FakeProc:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return "-1000\n"

        monkeypatch.setattr("builtins.open", FakeProc)
        writes = []
        monkeypatch.setattr(
            "quebec.supervisor._write_oom_score_adj",
            lambda pid, value: writes.append((pid, value)) or True,
        )
        assert _reset_child_oom_priority() is True
        assert writes == [(os.getpid(), CHILD_OOM_SCORE_ADJ)]

    def test_child_reset_fails_when_it_cannot_write(self, monkeypatch):
        class FakeProc:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return "-1000\n"

        monkeypatch.setattr("builtins.open", FakeProc)
        monkeypatch.setattr(
            "quebec.supervisor._write_oom_score_adj", lambda *_args: False
        )
        assert _reset_child_oom_priority() is False

    def test_child_reset_ignores_missing_proc_off_linux(self, monkeypatch):
        """No /proc, no OOM killer: nothing to be immune from."""

        def boom(*_args, **_kwargs):
            raise OSError("no /proc")

        monkeypatch.setattr("builtins.open", boom)
        monkeypatch.setattr(sys, "platform", "darwin")
        assert _reset_child_oom_priority() is True

    def test_child_reset_fails_on_linux_when_the_read_fails(self, monkeypatch):
        """A failed read proves nothing about what was inherited, and an
        unkillable worker is the one outcome that cannot be risked."""

        def boom(*_args, **_kwargs):
            raise OSError("seccomp")

        monkeypatch.setattr("builtins.open", boom)
        monkeypatch.setattr(sys, "platform", "linux")
        assert _reset_child_oom_priority() is False


class TestRuntimeAdjust:
    """Adjusting a slot's limits is immediate on a live child and sticky."""

    def test_adjust_rewrites_limits_on_a_live_leaf(self, tmp_path):
        root = make_root(tmp_path)
        mgr = CgroupManager(str(root))
        path = mgr.create(ROLE_WORKER, 0, Limits(memory_max=1024, memory_swap_max=0))

        mgr.adjust(
            ROLE_WORKER,
            0,
            Limits(
                memory_max=2048,
                memory_high=1024,
                memory_swap_max=cgroup.UNLIMITED,
                memory_oom_group=False,
            ),
        )

        assert (Path(path) / "memory.max").read_text() == "2048"
        assert (Path(path) / "memory.high").read_text() == "1024"
        assert (Path(path) / "memory.swap.max").read_text() == "max"
        assert (Path(path) / "memory.oom.group").read_text() == "0"

    def test_adjust_clears_a_limit_by_writing_max(self, tmp_path):
        root = make_root(tmp_path)
        mgr = CgroupManager(str(root))
        path = mgr.create(ROLE_WORKER, 0, Limits(memory_max=1024, memory_swap_max=0))

        mgr.adjust(ROLE_WORKER, 0, Limits(memory_max=cgroup.UNLIMITED))

        assert (Path(path) / "memory.max").read_text() == "max"

    def test_adjust_without_a_live_child_is_a_noop(self, tmp_path):
        mgr = CgroupManager(str(make_root(tmp_path)))
        mgr.adjust(ROLE_WORKER, 0, Limits(memory_max=1024))  # must not raise

    def test_supervisor_adjust_records_and_defers_to_the_loop(self):
        cg = MagicMock(enabled=True)
        sup, _qc = make_supervisor(cg, plan={ROLE_WORKER: 2})
        sup._slot_limits[(ROLE_WORKER, 0)] = Limits(memory_max=1024)

        new = sup.adjust_slot_limit(ROLE_WORKER, 0, memory_max="2G")

        assert new.memory_max == 2 * 1024**3
        assert sup._slot_limits[(ROLE_WORKER, 0)] is new
        assert sup._pending_adjusts[(ROLE_WORKER, 0)] is new
        cg.adjust.assert_not_called(), "the live write is deferred to the loop"

        sup._apply_pending_adjusts()
        cg.adjust.assert_called_once_with(ROLE_WORKER, 0, new)
        assert sup._pending_adjusts == {}

    def test_supervisor_adjust_parses_sizes_and_keeps_explicit_max(self):
        sup, _qc = make_supervisor(DisabledCgroup("t"), plan={ROLE_WORKER: 1})

        new = sup.adjust_slot_limit(
            ROLE_WORKER, 0, memory_max="512MiB", memory_swap_max="max"
        )

        assert new.memory_max == 512 * 1024**2
        assert new.memory_swap_max == cgroup.UNLIMITED
        assert new.memory_oom_group is True  # companion default

    def test_supervisor_adjust_clear_keeps_the_other_fields(self):
        sup, _qc = make_supervisor(DisabledCgroup("t"), plan={ROLE_WORKER: 1})
        sup._slot_limits[(ROLE_WORKER, 0)] = Limits(
            memory_max=1024, memory_swap_max=0, memory_oom_group=True
        )

        new = sup.adjust_slot_limit(ROLE_WORKER, 0, memory_max=None)

        assert new.memory_max == cgroup.UNLIMITED, "clearing writes `max`, not nothing"
        assert new.memory_swap_max == 0  # unchanged unless told otherwise
        assert new.memory_oom_group is True

    def test_supervisor_adjust_clear_oom_group_resets_to_default(self):
        sup, _qc = make_supervisor(DisabledCgroup("t"), plan={ROLE_WORKER: 1})
        sup._slot_limits[(ROLE_WORKER, 0)] = Limits(
            memory_max=1024, memory_oom_group=True
        )

        new = sup.adjust_slot_limit(ROLE_WORKER, 0, memory_oom_group=None)

        assert new.memory_oom_group is False

    def test_concurrent_adjusts_do_not_lose_a_field(self, monkeypatch):
        """Two threads tuning different fields both read the pre-change value
        unless the read-merge-record window is held, and the later write then
        silently reverts the earlier one."""
        sup, _qc = make_supervisor(
            EnabledNoopCgroup("t"), plan={ROLE_WORKER: 1}, limits={}
        )
        sup._slot_limits[(ROLE_WORKER, 0)] = Limits()
        reading = threading.Event()

        class SlowRead(dict):
            """Stretch the gap between reading the current limits and writing
            the merged ones, where a scheduler switch would otherwise land."""

            def get(self, key, default=None):
                current = super().get(key, default)
                if not reading.is_set():
                    reading.set()
                    time.sleep(0.05)
                return current

        sup._slot_limits = SlowRead(sup._slot_limits)

        thread = threading.Thread(
            target=lambda: sup.adjust_slot_limit(ROLE_WORKER, 0, memory_max="256MiB")
        )
        thread.start()
        assert reading.wait(2.0), "the first adjust never read the current limits"
        sup.adjust_slot_limit(ROLE_WORKER, 0, memory_high="128MiB")
        thread.join(2.0)

        final = sup._slot_limits[(ROLE_WORKER, 0)]
        assert final.memory_max == 256 * 1024 * 1024
        assert final.memory_high == 128 * 1024 * 1024

    def test_supervisor_adjust_rejects_bad_role_and_index(self):
        sup, _qc = make_supervisor(DisabledCgroup("t"), plan={ROLE_WORKER: 2})
        with pytest.raises(ValueError):
            sup.adjust_slot_limit("nope", 0, memory_max=1)
        with pytest.raises(IndexError):
            sup.adjust_slot_limit(ROLE_WORKER, 5, memory_max=1)

    def test_supervisor_adjust_rejects_bad_values(self):
        sup, _qc = make_supervisor(DisabledCgroup("t"), plan={ROLE_WORKER: 1})
        with pytest.raises(ValueError):
            sup.adjust_slot_limit(ROLE_WORKER, 0, memory_max="garbage")
        with pytest.raises(ValueError):
            sup.adjust_slot_limit(ROLE_WORKER, 0, memory_oom_group=1)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is Unix-only")
def test_a_forked_child_forgets_the_supervisor(monkeypatch):
    """A worker inherits a copy of the object but supervises nothing: handing
    it out would accept limit changes into dicts nothing ever drains."""
    read_fd, write_fd = os.pipe()
    sup, qc = make_supervisor(DisabledCgroup("t"), plan={ROLE_WORKER: 1}, limits={})
    monkeypatch.setattr("quebec.supervisor._active_supervisor", sup)

    def probe():
        os.write(write_fd, b"none" if current_supervisor() is None else b"stale")
        os.close(write_fd)
        os._exit(0)

    qc.reset_after_fork = probe
    sup._fork_child(ROLE_WORKER, 0)

    (child_pid,) = sup._children
    os.close(write_fd)
    try:
        data = os.read(read_fd, 1024)
    finally:
        os.close(read_fd)
    os.waitpid(child_pid, 0)
    assert data == b"none"


def test_current_supervisor_defaults_to_none():
    assert current_supervisor() is None


def test_supervisor_applies_the_pool_budget_to_the_cgroup():
    sup, _qc = make_supervisor(
        EnabledNoopCgroup("t"), plan={ROLE_WORKER: 1}, workers_pool_memory_max="7GiB"
    )
    assert sup._workers_pool_limits.memory_max == 7 * 1024**3
    assert sup._workers_pool_limits.memory_oom_group is False
    assert sup._cgroup.workers_pool_limits is sup._workers_pool_limits


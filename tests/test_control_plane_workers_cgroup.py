"""The workers page's cgroup column.

A process reports the counters of whatever cgroup it happens to be in. When no
per-worker leaves were created — non-root with no delegated subtree, a
read-only /sys/fs/cgroup, or plain single-process mode — every worker reports
the *same* cgroup, and each row then shows that whole cgroup's usage. Without
saying so the page reads as "each of these workers used 1.6 GiB".
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
import quebec
from sqlalchemy import text

BASE = "/quebec"


def _get(qc, path: str) -> tuple[int, str]:
    req = quebec.AsgiRequest("GET", path, "", [], b"", BASE)
    status, _headers, body = qc.handle_control_plane_request(req)
    return status, bytes(body).decode()


def _add_worker(env, *, pid: int, hostname: str, cgroup_path: str | None) -> None:
    """Insert a live worker row whose heartbeat carries cgroup counters."""
    metadata: dict = {"rss_bytes": 338 * 1024 * 1024, "cgroup_current_bytes": 1_700_000_000}
    if cgroup_path is not None:
        metadata["cgroup_path"] = cgroup_path
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    env["session"].execute(
        text(
            f'INSERT INTO "{env["prefix"]}_processes" '
            "(kind, name, pid, hostname, metadata, last_heartbeat_at, created_at) "
            "VALUES ('Worker', :name, :pid, :hostname, :metadata, :now, :now)"
        ),
        {
            "name": f"worker-{pid}",
            "pid": pid,
            "hostname": hostname,
            "metadata": json.dumps(metadata),
            "now": now,
        },
    )
    env["session"].commit()


@pytest.fixture
def env(qc_with_sqlalchemy):
    return qc_with_sqlalchemy


def test_workers_sharing_one_cgroup_are_marked_shared(env) -> None:
    for pid in (101, 102, 103):
        _add_worker(env, pid=pid, hostname="box", cgroup_path="/system.slice/quebec.service")

    status, html = _get(env["qc"], "/workers")

    assert status == 200
    assert "shared &times;3" in html or "shared ×3" in html
    # The reason has to be visible, not just the badge.
    assert "not this process's" in html


def test_workers_in_their_own_leaves_are_not_marked(env) -> None:
    """The supervisor gives each worker a leaf of its own; then the counters
    really are per-worker and the badge must stay away."""
    for pid, leaf in ((201, "worker-0"), (202, "worker-1")):
        _add_worker(
            env,
            pid=pid,
            hostname="box",
            cgroup_path=f"/system.slice/quebec.service/workers/{leaf}",
        )

    status, html = _get(env["qc"], "/workers")

    assert status == 200
    assert "shared" not in html


def test_the_same_path_on_two_hosts_is_two_cgroups(env) -> None:
    """Identical paths on different machines are unrelated cgroups — counting
    by path alone would call them shared."""
    _add_worker(env, pid=301, hostname="box-a", cgroup_path="/system.slice/quebec.service")
    _add_worker(env, pid=302, hostname="box-b", cgroup_path="/system.slice/quebec.service")

    status, html = _get(env["qc"], "/workers")

    assert status == 200
    assert "shared" not in html


def test_a_worker_without_cgroup_counters_is_unaffected(env) -> None:
    """A non-cgroup host reports no path; the column falls back to N/A and
    nothing claims sharing."""
    _add_worker(env, pid=401, hostname="box", cgroup_path=None)

    status, html = _get(env["qc"], "/workers")

    assert status == 200
    assert "shared" not in html
